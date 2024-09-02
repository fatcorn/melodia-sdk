package tasks

/*
#include <pthread.h>
#include <stdio.h>

void printThreadID() {
    pthread_t tid = pthread_self();
    printf("Current Thread ID: %lu\n", (unsigned long)tid);
}
*/
import "C"
import (
	"context"
	"cosmossdk.io/store"
	"fmt"
	abci "github.com/cometbft/cometbft/abci/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/docker"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"strconv"
	"sync"
	"time"

	"cosmossdk.io/store/multiversion"
	storetypes "cosmossdk.io/store/types"
	"cosmossdk.io/store/types/occ"
)

type status string

const (
	// statusPending tasks are ready for execution
	// all executing tasks are in pending state
	statusPending status = "pending"
	// statusExecuted tasks are ready for validation
	// these tasks did not abort during execution
	statusExecuted status = "executed"
	// statusAborted means the task has been aborted
	// these tasks transition to pending upon next execution
	statusAborted status = "aborted"
	// statusValidated means the task has been validated
	// tasks in this status can be reset if an earlier task fails validation
	statusValidated status = "validated"
	// statusWaiting tasks are waiting for another tx to complete
	statusWaiting status = "waiting"
	// maximumIterations before we revert to sequential (for high conflict rates)
	maximumIterations = 10
)

var threadProfile *pprof.Profile

type deliverTxTask struct {
	Ctx     sdk.Context
	AbortCh chan occ.Abort

	mx            sync.RWMutex
	Status        status
	Dependencies  map[int]struct{}
	Abort         *occ.Abort
	Incarnation   int
	Request       []byte
	SdkTx         sdk.Tx
	Checksum      [32]byte
	AbsoluteIndex int
	Response      *abci.ExecTxResult
	VersionStores map[storetypes.StoreKey]*multiversion.VersionIndexedStore
	IsEVMTx       bool
	NamespaceId   uint64
	TotalTask     int
	//TxTracer      sdk.TxTracer
}

// AppendDependencies appends the given indexes to the task's dependencies
func (dt *deliverTxTask) AppendDependencies(deps []int) {
	dt.mx.Lock()
	defer dt.mx.Unlock()
	for _, taskIdx := range deps {
		dt.Dependencies[taskIdx] = struct{}{}
	}
}

func (dt *deliverTxTask) IsStatus(s status) bool {
	dt.mx.RLock()
	defer dt.mx.RUnlock()
	return dt.Status == s
}

func (dt *deliverTxTask) SetStatus(s status) {
	dt.mx.Lock()
	defer dt.mx.Unlock()
	dt.Status = s
}

func (dt *deliverTxTask) Reset() {
	dt.SetStatus(statusPending)
	dt.Response = nil
	dt.Abort = nil
	dt.AbortCh = nil
	dt.VersionStores = nil

	//if dt.TxTracer != nil {
	//	dt.TxTracer.Reset()
	//}
}

func (dt *deliverTxTask) Increment() {
	dt.Incarnation++
}

// Scheduler processes tasks concurrently
type Scheduler interface {
	ProcessAll(ctx sdk.Context, req sdk.DeliverTxBatchRequest) ([]abci.ExecTxResult, error)
	SetSchedulerHandlers(transactionAddressHandler sdk.TransactionAddressHandler, setMsgsHandler sdk.SetMsgsHandler, modifyLogIndexHandler sdk.ModifyLogIndexHandler, setBlockBloomTransientHandler sdk.SetBlockBloomTransientHandler, contractStaticAnalysisHandler sdk.ContractStaticAnalysisHandler, isEVMTxHandler sdk.IsEVMTxHandler)
}

// tx []byte
type scheduler struct {
	//deliverTx          func(ctx sdk.Context, req []byte, tx sdk.Tx, checksum [32]byte) (res abci.ExecTxResult)
	deliverTx                     func(tx []byte, opts ...sdk.Context) (res *abci.ExecTxResult)
	workers                       int
	multiVersionStores            map[storetypes.StoreKey]multiversion.MultiVersionStore
	allTasksMap                   map[int]*deliverTxTask
	allTasks                      []*deliverTxTask
	executeCh                     chan func()
	validateCh                    chan func()
	collectCh                     chan func()
	metrics                       *schedulerMetrics
	synchronous                   bool // true if maxIncarnation exceeds threshold
	maxIncarnation                int  // current highest incarnation
	transactionAddressHandler     sdk.TransactionAddressHandler
	setMsgsHandler                sdk.SetMsgsHandler
	modifyLogIndexHandler         sdk.ModifyLogIndexHandler
	setBlockBloomTransientHandler sdk.SetBlockBloomTransientHandler
	contractStaticAnalysisHandler sdk.ContractStaticAnalysisHandler
	isEVMTxHandler                sdk.IsEVMTxHandler
	namespaceList                 []uint64
	threadIds                     []int
	//tracingInfo        *tracing.Info

}

// NewScheduler creates a new scheduler
// func NewScheduler(workers int, deliverTxFunc func(ctx sdk.Context, req []byte, tx sdk.Tx, checksum [32]byte) (res abci.ExecTxResult)) Scheduler {
func NewScheduler(workers int, deliverTxFunc func(tx []byte, opts ...sdk.Context) *abci.ExecTxResult, namespaceList []uint64) Scheduler {
	threadProfile = pprof.Lookup("threadcreate")

	return &scheduler{
		workers:       workers,
		deliverTx:     deliverTxFunc,
		metrics:       &schedulerMetrics{},
		namespaceList: namespaceList,
	}
}
func (s *scheduler) SetSchedulerHandlers(transactionAddressHandler sdk.TransactionAddressHandler, setMsgsHandler sdk.SetMsgsHandler, modifyLogIndexHandler sdk.ModifyLogIndexHandler, setBlockBloomTransientHandler sdk.SetBlockBloomTransientHandler, contractStaticAnalysisHandler sdk.ContractStaticAnalysisHandler, isEVMTxHandler sdk.IsEVMTxHandler) {
	s.transactionAddressHandler = transactionAddressHandler
	s.setMsgsHandler = setMsgsHandler
	s.modifyLogIndexHandler = modifyLogIndexHandler
	s.setBlockBloomTransientHandler = setBlockBloomTransientHandler
	s.contractStaticAnalysisHandler = contractStaticAnalysisHandler
	s.isEVMTxHandler = isEVMTxHandler
}

func (s *scheduler) invalidateTask(task *deliverTxTask) {
	for _, mv := range s.multiVersionStores {
		mv.InvalidateWriteset(task.AbsoluteIndex, task.Incarnation, task.TotalTask)
		mv.ClearReadset(task.AbsoluteIndex)
		mv.ClearIterateset(task.AbsoluteIndex)
	}
}

func (s *scheduler) start(ctx context.Context, ch chan func(), workers int) {
	for i := 0; i < workers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case work := <-ch:
					//printThreadID()
					work()
				}
			}
		}()
	}
}

func (s *scheduler) startVal(ctx context.Context, ch chan func(), workers int) {
	for i := 0; i < workers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case work := <-ch:
					//printThreadID()
					work()
				}
			}
		}()
	}
}

func (s *scheduler) DoValidate(work func()) {
	if s.synchronous {
		work()
		return
	}
	s.validateCh <- work
}

func (s *scheduler) DoExecute(work func()) {
	if s.synchronous {
		work()
		return
	}
	s.executeCh <- work
}

func (s *scheduler) DoWrite(work func()) {
	if s.synchronous {
		work()
		return
	}
	s.collectCh <- work
}

func (s *scheduler) findConflicts(task *deliverTxTask) (bool, []int) {
	var conflicts []int
	uniq := make(map[int]struct{})
	valid := true
	for _, mv := range s.multiVersionStores {
		//fmt.Println("storeKey: ", k)
		//if k.Name() == "feemarket" {
		//	continue
		//}
		//if k.Name() == "transient:evm" {
		//	continue
		//}
		ok, mvConflicts := mv.ValidateTransactionState(task.AbsoluteIndex)
		for _, c := range mvConflicts {
			if _, ok := uniq[c]; !ok {
				conflicts = append(conflicts, c)
				uniq[c] = struct{}{}
			}
		}
		// any non-ok value makes valid false
		valid = valid && ok
	}
	sort.Ints(conflicts)
	// print task's absolute index and conflicts
	//fmt.Printf("task's absolute index: %d, conflicts: %v\n", task.AbsoluteIndex, conflicts)
	return valid, conflicts
}

func toTasks(ctx sdk.Context, req sdk.DeliverTxBatchRequest, addressHandler sdk.TransactionAddressHandler) ([]*deliverTxTask, map[int]*deliverTxTask) {
	reqs := req.TxEntries
	orders := req.Orders
	tasksMap := make(map[int]*deliverTxTask)
	allTasks := make([]*deliverTxTask, 0, len(reqs))
	totalTask := len(reqs)

	addTxTask := func(dependIdx int, r *sdk.DeliverTxEntry, status status) {
		task := &deliverTxTask{
			Request:       r.Request,
			SdkTx:         r.SdkTx,
			Checksum:      r.Checksum,
			AbsoluteIndex: r.AbsoluteIndex,
			Status:        statusPending,
			Dependencies:  map[int]struct{}{},
			TotalTask:     totalTask,
			IsEVMTx:       false,

			//TxTracer:      r.TxTracer,
		}

		// if there are dependencies, set the task to waiting
		if status == statusWaiting {
			dependencies := map[int]struct{}{}
			dependencies[dependIdx] = struct{}{}
			task.Status = statusWaiting
			task.Dependencies = dependencies
		}
		tasksMap[r.AbsoluteIndex] = task
		allTasks = append(allTasks, task)
	}

	if orders.NsTransactionOrders != nil {
		for _, orderEntry := range orders.NsTransactionOrders.Entries {
			for _, idxes := range orderEntry.Indexes {
				for i, idx := range idxes.Indexes {
					if i > 0 {
						addTxTask(int(idxes.Indexes[i-1]), reqs[idx], statusWaiting)
					} else {
						addTxTask(-1, reqs[idx], statusPending)
					}
				}
			}
		}
	}
	if len(allTasks) != len(reqs) {
		panic(fmt.Errorf("toTasks NsTransactionOrders total.Size(%s) != txs.Size(%s)", len(allTasks), len(reqs)))
	}

	return allTasks, tasksMap
}

func (s *scheduler) collectResponses(ctx sdk.Context, tasks []*deliverTxTask) ([]abci.ExecTxResult, []sdk.Msg) {
	res := make([]abci.ExecTxResult, 0, len(tasks))
	evmTxs := make([]sdk.Msg, len(tasks))

	evmTxIndex := 0

	logSize := uint64(0)
	txLogs := make([]string, 0)

	// start time
	startTime := time.Now()

	for _, t := range tasks {
		res = append(res, *t.Response)

		t.IsEVMTx = s.isEVMTxHandler(ctx, t.SdkTx)

		if !t.IsEVMTx {
			continue
		}

		for _, e := range t.Response.Events {

			if e.Type == "ethereum_tx" {
				for i := range e.Attributes {
					if e.Attributes[i].Key != "txIndex" {
						continue
					}
					e.Attributes[i].Value = strconv.Itoa(evmTxIndex)

					break
				}
			} else if e.Type == "tx_log" {
				for i := range e.Attributes {
					if e.Attributes[i].Key != "txLog" {
						continue
					}
					newLog := s.modifyLogIndexHandler(ctx, e.Attributes[i].Value, logSize)
					e.Attributes[i].Value = newLog
					txLogs = append(txLogs, newLog)
					logSize++
				}
			} else if e.Type == "message" {
				var isEVM bool
				for _, attr := range e.Attributes {
					if attr.Key == "module" && attr.Value == "evm" {
						isEVM = true
						break
					}
				}

				if isEVM {
					for _, attr := range e.Attributes {
						if attr.Key == "sender" {
							s.contractStaticAnalysisHandler(ctx, t.SdkTx, attr.Value)
							break
						}
					}
				}
			}
		}
		evmTxIndex++
		if t.IsEVMTx {
			evmTxs[t.AbsoluteIndex] = t.SdkTx.GetMsgs()[0]
		} else {
			evmTxs[t.AbsoluteIndex] = nil
		}

		//if t.TxTracer != nil {
		//	t.TxTracer.Commit()
		//}
	}
	if logSize > 0 {
		s.setBlockBloomTransientHandler(ctx, txLogs)
	}
	// calculate time used
	fmt.Printf("========= Time used: %v\n", time.Since(startTime))
	return res, evmTxs
}

func (s *scheduler) tryInitMultiVersionStore(ctx sdk.Context, totalTask int) {
	if s.multiVersionStores != nil {
		return
	}
	mvs := make(map[storetypes.StoreKey]multiversion.MultiVersionStore)
	keys := ctx.MultiStore().StoreKeys()
	listStore := os.Getenv("listStore")

	for _, sk := range keys {
		if listStore != "" {
			mvs[sk] = multiversion.NewMultiVersionListStore(ctx.MultiStore().GetKVStore(sk), totalTask)
		} else {
			mvs[sk] = multiversion.NewMultiVersionStore(ctx.MultiStore().GetKVStore(sk))
		}
	}
	s.multiVersionStores = mvs
}

func dependenciesValidated(tasksMap map[int]*deliverTxTask, deps map[int]struct{}) bool {
	for i := range deps {
		// because idx contains absoluteIndices, we need to fetch from map
		task := tasksMap[i]
		if !task.IsStatus(statusValidated) {
			return false
		}
	}
	return true
}

func filterTasks(tasks []*deliverTxTask, filter func(*deliverTxTask) bool) []*deliverTxTask {
	var res []*deliverTxTask
	for _, t := range tasks {
		if filter(t) {
			res = append(res, t)
		}
	}
	return res
}

func allValidated(tasks []*deliverTxTask) bool {
	for _, t := range tasks {
		if !t.IsStatus(statusValidated) {
			return false
		}
	}
	return true
}

func (s *scheduler) PrefillEstimates(reqs []*sdk.DeliverTxEntry) {
	// iterate over TXs, update estimated writesets where applicable
	for _, req := range reqs {
		mappedWritesets := req.EstimatedWritesets
		// order shouldnt matter for storeKeys because each storeKey partitioned MVS is independent
		for storeKey, writeset := range mappedWritesets {
			// we use `-1` to indicate a prefill incarnation
			s.multiVersionStores[storeKey].SetEstimatedWriteset(req.AbsoluteIndex, -1, writeset)
		}
	}
}

// schedulerMetrics contains metrics for the scheduler
type schedulerMetrics struct {
	// maxIncarnation is the highest incarnation seen in this set
	maxIncarnation int
	// retries is the number of tx attempts beyond the first attempt
	retries int
}

//func (s *scheduler) emitMetrics() {
//	telemetry.IncrCounter(float32(s.metrics.retries), "scheduler", "retries")
//	telemetry.IncrCounter(float32(s.metrics.maxIncarnation), "scheduler", "incarnations")
//}

func (s *scheduler) ProcessAll(ctx sdk.Context, req sdk.DeliverTxBatchRequest) ([]abci.ExecTxResult, error) {
	startTime := time.Now()
	var iterations int
	// initialize mutli-version stores if they haven't been initialized yet
	s.tryInitMultiVersionStore(ctx, len(req.TxEntries))
	// prefill estimates
	// This "optimization" path is being disabled because we don't have a strong reason to have it given that it
	//s.PrefillEstimates(reqs)
	toTaskStartTime := time.Now()
	tasks, tasksMap := toTasks(ctx, req, s.transactionAddressHandler)
	fmt.Printf("========= toTasks Time used: %v\n", time.Since(toTaskStartTime))
	s.allTasks = tasks
	s.allTasksMap = tasksMap
	s.executeCh = make(chan func(), len(tasks))
	s.validateCh = make(chan func(), len(tasks))
	s.collectCh = make(chan func(), len(tasks))
	//defer s.emitMetrics()

	// default to number of tasks if workers is negative or 0 by this point
	workers := s.workers
	if s.workers < 1 || len(tasks) < s.workers {
		workers = len(tasks)
	}

	workerCtx, cancel := context.WithCancel(ctx.Context())
	defer cancel()
	// set trace store file
	now := time.Now().UnixMilli()
	nowTime := strconv.Itoa(int(now))
	tracePath := "trace" + nowTime + ".out"
	tracePathEnv := os.Getenv("tracePath")
	if tracePathEnv != "" {
		tracePath = tracePathEnv + tracePath
	}
	f, _ := os.Create(tracePath)

	// start trace
	if err := trace.Start(f); err != nil {
		panic(err)
	}
	//go printCpuCount()

	// execution tasks are limited by workers
	s.start(workerCtx, s.executeCh, workers)

	s.start(workerCtx, s.collectCh, workers)

	toExecute := tasks
	for !allValidated(tasks) {
		// if the max incarnation >= x, we should revert to synchronous
		if iterations >= maximumIterations {
			// process synchronously
			s.synchronous = true
			startIdx, anyLeft := s.findFirstNonValidated()
			if !anyLeft {
				break
			}
			toExecute = tasks[startIdx:]
		}
		//go printCpuCount()
		//printSystemStats("start ")

		// execute sets statuses of tasks to either executed or aborted
		if err := s.executeAll(ctx, toExecute); err != nil {
			return nil, err
		}

		//writeNow := cmttime.Now()
		// write from version store to multiversion stores

		totalExecuted := 0
		for _, task := range toExecute {
			if task.IsStatus(statusExecuted) {
				totalExecuted++
				for _, v := range task.VersionStores {
					writeset := v.GetWriteset()
					for key, _ := range writeset {
						v.NewKey(key, task.TotalTask)
					}
				}
			}
		}

		wg := &sync.WaitGroup{}
		wg.Add(totalExecuted)

		for _, task := range toExecute {
			if task.IsStatus(statusExecuted) {
				s.DoWrite(func() {
					println("execute write to multi version store", "index", task.AbsoluteIndex)

					for _, v := range task.VersionStores {
						v.WriteToMultiVersionStore()
					}
					defer wg.Done()

				})
			}
		}
		wg.Wait()

		//writeSleepDuration := cmttime.Now().Sub(writeNow)
		//println("WriteToMultiVersionStore time: ", writeSleepDuration, " from: ", writeNow.String())

		// validate returns any that should be re-executed
		// note this processes ALL tasks, not just those recently executed
		// validation tasks uses length of tasks to avoid blocking on validation
		s.startVal(workerCtx, s.validateCh, len(tasks))

		var err error
		toExecute, err = s.validateAll(ctx, tasks)
		//printSystemStats("end ")

		if err != nil {
			return nil, err
		}
		// these are retries which apply to metrics
		s.metrics.retries += len(toExecute)

		iterations++
	}
	for _, mv := range s.multiVersionStores {
		mv.WriteLatestToStore()
	}
	// end trace
	trace.Stop()
	s.metrics.maxIncarnation = s.maxIncarnation

	ctx.Logger().Info("occ scheduler", "height", ctx.BlockHeight(), "txs", len(tasks), "latency_ms", time.Since(startTime).Milliseconds(), "retries", s.metrics.retries, "maxIncarnation", s.maxIncarnation, "iterations", iterations, "sync", s.synchronous, "workers", workers)

	res, evmTxs := s.collectResponses(ctx, tasks)
	//printSystemStats("after process")

	s.setMsgsHandler(ctx, evmTxs)

	return res, nil
}

func (s *scheduler) shouldRerun(task *deliverTxTask) bool {
	switch task.Status {

	case statusAborted, statusPending:
		return true

	// validated tasks can become unvalidated if an earlier re-run task now conflicts
	case statusExecuted, statusValidated:
		// With the current scheduler, we won't actually get to this step if a previous task has already been determined to be invalid,
		// since we choose to fail fast and mark the subsequent tasks as invalid as well.
		// TODO: in a future async scheduler that no longer exhaustively validates in order, we may need to carefully handle the `valid=true` with conflicts case
		if valid, conflicts := s.findConflicts(task); !valid {
			s.invalidateTask(task)
			task.AppendDependencies(conflicts)

			// if the conflicts are now validated, then rerun this task
			if dependenciesValidated(s.allTasksMap, task.Dependencies) {
				return true
			} else {
				// otherwise, wait for completion
				task.SetStatus(statusWaiting)
				return false
			}
		} else if len(conflicts) == 0 {
			// mark as validated, which will avoid re-validating unless a lower-index re-validates
			task.SetStatus(statusValidated)
			return false
		}
		// conflicts and valid, so it'll validate next time
		return false

	case statusWaiting:
		// if conflicts are done, then this task is ready to run again
		return dependenciesValidated(s.allTasksMap, task.Dependencies)
	}
	panic("unexpected status: " + task.Status)
}

func (s *scheduler) validateTask(ctx sdk.Context, task *deliverTxTask) bool {
	//_, span := s.traceSpan(ctx, "SchedulerValidate", task)
	//defer span.End()

	if s.shouldRerun(task) {
		return false
	}
	return true
}

func (s *scheduler) findFirstNonValidated() (int, bool) {
	for i, t := range s.allTasks {
		if t.Status != statusValidated {
			return i, true
		}
	}
	return 0, false
}

func (s *scheduler) validateAll(ctx sdk.Context, tasks []*deliverTxTask) ([]*deliverTxTask, error) {
	//ctx, span := s.traceSpan(ctx, "SchedulerValidateAll", nil)
	//defer span.End()

	var mx sync.Mutex
	var res []*deliverTxTask

	startIdx, anyLeft := s.findFirstNonValidated()

	if !anyLeft {
		return nil, nil
	}

	wg := &sync.WaitGroup{}
	for i := startIdx; i < len(tasks); i++ {
		wg.Add(1)
		t := tasks[i]
		s.DoValidate(func() {
			defer wg.Done()
			if !s.validateTask(ctx, t) {
				mx.Lock()
				defer mx.Unlock()
				t.Reset()
				t.Increment()
				// update max incarnation for scheduler
				if t.Incarnation > s.maxIncarnation {
					s.maxIncarnation = t.Incarnation
				}
				res = append(res, t)
			}
		})
	}
	//printSystemStats("validating")

	wg.Wait()

	return res, nil
}

// ExecuteAll executes all tasks concurrently
func (s *scheduler) executeAll(ctx sdk.Context, tasks []*deliverTxTask) error {
	if len(tasks) == 0 {
		return nil
	}
	//ctx, span := s.traceSpan(ctx, "SchedulerExecuteAll", nil)
	//span.SetAttributes(attribute.Bool("synchronous", s.synchronous))
	//defer span.End()

	// validationWg waits for all validations to complete
	// validations happen in separate goroutines in order to wait on previous index
	wg := &sync.WaitGroup{}

	for _, task := range tasks {
		if !s.synchronous && task.IsStatus(statusWaiting) {
			continue
		}
		t := task
		wg.Add(1)
		s.DoExecute(func() {
			s.prepareAndRunTask(wg, ctx, t)
		})
	}
	//printSystemStats("executing")

	wg.Wait()

	return nil
}
func (s *scheduler) prepareAndRunTask(wg *sync.WaitGroup, ctx sdk.Context, task *deliverTxTask) {
	//eCtx, eSpan := s.traceSpan(ctx, "SchedulerExecute", task)
	//defer eSpan.End()
	defer wg.Done()

	//C.printThreadID() // 打印线程ID
	//now := cmttime.Now()
	task.Ctx = ctx
	s.executeTask(task)
	if s.synchronous {
		for _, v := range task.VersionStores {
			writeset := v.GetWriteset()
			for key, _ := range writeset {
				v.NewKey(key, task.TotalTask)
			}
			println("prepare write to multi version store", "index", task.AbsoluteIndex)
			v.WriteToMultiVersionStore()
		}
	}

	//wg.Add(1)
	//s.DoWrite(func() {
	//	defer wg.Done()
	//	for _, v := range task.VersionStores {
	//		v.WriteToMultiVersionStore()
	//	}
	//})

	//sleepDuration := cmttime.Now().Sub(now)
	//println("prepareAndRunTask task: ", task.AbsoluteIndex, " time: ", sleepDuration, " from: ", now.String())
}

//func (s *scheduler) traceSpan(ctx sdk.Context, name string, task *deliverTxTask) (sdk.Context, trace.Span) {
//	spanCtx, span := s.tracingInfo.StartWithContext(name, ctx.TraceSpanContext())
//	if task != nil {
//		span.SetAttributes(attribute.String("txHash", fmt.Sprintf("%X", sha256.Sum256(task.Request.Tx))))
//		span.SetAttributes(attribute.Int("absoluteIndex", task.AbsoluteIndex))
//		span.SetAttributes(attribute.Int("txIncarnation", task.Incarnation))
//	}
//	ctx = ctx.WithTraceSpanContext(spanCtx)
//	return ctx, span
//}

// prepareTask initializes the context and version stores for a task
func (s *scheduler) prepareTask(task *deliverTxTask) {
	ctx := task.Ctx.WithTxIndex(task.AbsoluteIndex)

	//_, span := s.traceSpan(ctx, "SchedulerPrepare", task)
	//defer span.End()

	// initialize the context
	abortCh := make(chan occ.Abort, len(s.multiVersionStores))

	// print len(s.multiVersionStores)

	// if there are no stores, don't try to wrap, because there's nothing to wrap
	if len(s.multiVersionStores) > 0 {
		// non-blocking
		cms := ctx.MultiStore().CacheMultiStore()

		// init version stores by store key
		vs := make(map[storetypes.StoreKey]*multiversion.VersionIndexedStore)
		for storeKey, mvs := range s.multiVersionStores {
			vs[storeKey] = mvs.VersionedIndexedStore(task.AbsoluteIndex, task.Incarnation, abortCh, task.TotalTask)
		}

		// save off version store so we can ask it things later
		task.VersionStores = vs
		ms := cms.SetKVStores(func(k storetypes.StoreKey, kvs storetypes.KVStore) store.CacheWrap {
			return vs[k]
		})

		ctx = ctx.WithMultiStore(ms)
		namespaceId := s.namespaceList[task.AbsoluteIndex]
		ctx = ctx.WithNamepace(namespaceId)
		task.NamespaceId = namespaceId
	}

	task.AbortCh = abortCh
	task.Ctx = ctx
}

func (s *scheduler) executeTask(task *deliverTxTask) {
	//dCtx, dSpan := s.traceSpan(task.Ctx, "SchedulerExecuteTask", task)
	//defer dSpan.End()
	//task.Ctx = dCtx

	// in the synchronous case, we only want to re-execute tasks that need re-executing
	if s.synchronous {
		// if already validated, then this does another validation
		if task.IsStatus(statusValidated) {
			s.shouldRerun(task)
			if task.IsStatus(statusValidated) {
				return
			}
		}

		// waiting transactions may not yet have been reset
		// this ensures a task has been reset and incremented
		if !task.IsStatus(statusPending) {
			task.Reset()
			task.Increment()
		}
	}
	//prepareTaskNow := cmttime.Now()

	s.prepareTask(task)
	//prepareTaskSleepDuration := cmttime.Now().Sub(prepareTaskNow)
	//println("prepareTaskNow task: ", task.AbsoluteIndex, " time: ", prepareTaskSleepDuration, " from: ", prepareTaskNow.String())

	//now := cmttime.Now()
	//t0 := time.Now()
	//now := cmttime.Now()
	resp := s.deliverTx(task.Request, task.Ctx)
	//t1 := time.Now()
	//println("deliver tx=============", "start time", t0.String(), "end time", t1.String(), "sub", t1.Sub(t0).String()) //sleepDuration := cmttime.Now().Sub(now)
	//println("deliverTx task: ", task.AbsoluteIndex, " time: ", sleepDuration, " from: ", now.String())

	// close the abort channel
	close(task.AbortCh)
	abort, ok := <-task.AbortCh

	//estimateNow := cmttime.Now()
	if ok {
		// if there is an abort item that means we need to wait on the dependent tx
		task.SetStatus(statusAborted)
		task.Abort = &abort
		task.AppendDependencies([]int{abort.DependentTxIdx})
		// write from version store to multiversion stores
		for _, v := range task.VersionStores {
			println(" set estimates =======", "index", abort.DependentTxIdx)
			v.WriteEstimatesToMultiVersionStore()
		}
		return
	}
	//estimateSleepDuration := cmttime.Now().Sub(estimateNow)
	//println("EstimateSleepDuration task: ", task.AbsoluteIndex, " time: ", estimateSleepDuration, " from: ", estimateNow.String())

	task.SetStatus(statusExecuted)
	task.Response = resp

	//writeNow := cmttime.Now()
	//// write from version store to multiversion stores
	//for _, v := range task.VersionStores {
	//	v.WriteToMultiVersionStore()
	//}
	//for _, v := range task.VersionStores {
	//	v.WriteToMultiVersionStore()
	//}
	//if task.AbsoluteIndex%50 == 0 {
	//	printSystemStats("task==")
	//}
	//writeSleepDuration := cmttime.Now().Sub(writeNow)
	//println("WriteToMultiVersionStore task: ", task.AbsoluteIndex, " time: ", writeSleepDuration, " from: ", writeNow.String())
}
func printCpuCount() {
	for i := 0; i < 5; i++ {
		printCpu()
		//now := time.Now().UnixMilli()
		//nowTime := strconv.Itoa(int(now))
		//tracePath := "heap_profile" + nowTime + ".prof"
		//f, err := os.Create(tracePath)
		//if err != nil {
		//	fmt.Println("Could not create heap profile: ", err)
		//	return
		//}
		//
		//// 生成堆内存快照
		//if err := pprof.WriteHeapProfile(f); err != nil {
		//	fmt.Println("Could not write heap profile: ", err)
		//	return
		//}
		//fmt.Println("Heap profile saved to heap_profile.prof")
		time.Sleep(300 * time.Millisecond)
	}

}

func printSystemStats(desc string) {
	fmt.Printf("Stage: %s\n", desc)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Goroutines: %d\n", runtime.NumGoroutine())
	fmt.Printf("CPUs: %d\n", runtime.NumCPU())
	fmt.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))
	fmt.Printf("Threads: %d\n", threadProfile.Count())
	fmt.Printf("Memory Alloc = %v MiB\n", m.Alloc/1024/1024)
	fmt.Printf("TotalAlloc = %v MiB\n", m.TotalAlloc/1024/1024)
	fmt.Printf("Sys = %v MiB\n", m.Sys/1024/1024)
	fmt.Printf("NumGC = %v\n", m.NumGC)
	printCpu()
}

func printCpu() {

	percent, err := cpu.Percent(time.Millisecond*10, false)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("CPU percent: %.2f%%\n", percent[0])

}

func GetCpuPercent(perCpu bool) []float64 {
	percent, _ := cpu.Percent(time.Millisecond, perCpu)

	return percent
}

func GetDockerCpuPercent() {
	stats, err := docker.GetDockerStat()
	if err != nil {
		println("get docker stat err", "", err.Error())
	}
	for _, stat := range stats {
		println("docker cpu", "docker id", stat.String())

		cpuDocker, err := docker.CgroupCPUDocker(stat.ContainerID)
		if err != nil {
			println("get cpu docker err", "", err.Error())
		}
		println("docker cpu", "docker id", stat.String(), "cpu", cpuDocker.String())
	}
}
