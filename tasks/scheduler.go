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
	"github.com/cosmos/cosmos-sdk/types/errors"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/docker"
	"os"
	"runtime/pprof"
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

	mx               sync.RWMutex
	Status           status
	Dependencies     map[int]struct{}
	Abort            *occ.Abort
	Incarnation      int
	Request          []byte
	SdkTx            sdk.Tx
	Checksum         [32]byte
	AbsoluteIndex    int
	Response         *abci.ExecTxResult
	VersionStores    map[storetypes.StoreKey]*multiversion.VersionIndexedStore
	IsEVMTx          bool
	NamespaceId      uint64
	TotalTask        int
	AccountNumbers   map[string]map[uint64]uint64
	ExecutionCounter uint64
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
	SetSchedulerHandlers(modifyLogIndexHandler sdk.ModifyLogIndexHandler, setBlockBloomTransientHandler sdk.SetBlockBloomTransientHandler, isEVMTxHandler sdk.IsEVMTxHandler, nextAccountNumberHandler sdk.NextAccountNumberHandler, txAccountInfoHandler sdk.TxAccountInfoHandler, addTransientGasWanted sdk.AddTransientGasWanted, txNewAccountAddressesHandler sdk.TxNewAccountAddressesHandler, moveExecutionCounterHandler sdk.MoveExecutionCounterHandler)
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
	writeLatest                   chan func()
	metrics                       *schedulerMetrics
	synchronous                   bool // true if maxIncarnation exceeds threshold
	maxIncarnation                int  // current highest incarnation
	modifyLogIndexHandler         sdk.ModifyLogIndexHandler
	setBlockBloomTransientHandler sdk.SetBlockBloomTransientHandler
	isEVMTxHandler                sdk.IsEVMTxHandler
	nextAccountNumberHandler      sdk.NextAccountNumberHandler
	txNewAccountAddressesHandler  sdk.TxNewAccountAddressesHandler
	txAccountInfoHandler          sdk.TxAccountInfoHandler
	addTransientGasWanted         sdk.AddTransientGasWanted
	moveExecutionCounterHandler   sdk.MoveExecutionCounterHandler
	namespaceList                 []uint64
	threadIds                     []int
	//tracingInfo        *tracing.Info
	allNextAccountNumbers map[string]map[uint64]uint64
}

// NewScheduler creates a new scheduler
// func NewScheduler(workers int, deliverTxFunc func(ctx sdk.Context, req []byte, tx sdk.Tx, checksum [32]byte) (res abci.ExecTxResult)) Scheduler {
func NewScheduler(workers int, deliverTxFunc func(tx []byte, opts ...sdk.Context) *abci.ExecTxResult, namespaceList []uint64) Scheduler {
	threadProfile = pprof.Lookup("threadcreate")

	return &scheduler{
		workers:               workers,
		deliverTx:             deliverTxFunc,
		metrics:               &schedulerMetrics{},
		namespaceList:         namespaceList,
		allNextAccountNumbers: make(map[string]map[uint64]uint64),
	}
}
func (s *scheduler) SetSchedulerHandlers(modifyLogIndexHandler sdk.ModifyLogIndexHandler, setBlockBloomTransientHandler sdk.SetBlockBloomTransientHandler, isEVMTxHandler sdk.IsEVMTxHandler, nextAccountNumberHandler sdk.NextAccountNumberHandler, txAccountInfoHandler sdk.TxAccountInfoHandler, addTransientGasWanted sdk.AddTransientGasWanted, txNewAccountAddressesHandler sdk.TxNewAccountAddressesHandler, moveExecutionCounterHandler sdk.MoveExecutionCounterHandler) {
	s.modifyLogIndexHandler = modifyLogIndexHandler
	s.setBlockBloomTransientHandler = setBlockBloomTransientHandler
	s.isEVMTxHandler = isEVMTxHandler
	s.nextAccountNumberHandler = nextAccountNumberHandler
	s.txAccountInfoHandler = txAccountInfoHandler
	s.addTransientGasWanted = addTransientGasWanted
	s.txNewAccountAddressesHandler = txNewAccountAddressesHandler
	s.moveExecutionCounterHandler = moveExecutionCounterHandler
}

func (s *scheduler) invalidateTask(task *deliverTxTask) {
	for _, mv := range s.multiVersionStores {
		mv.InvalidateWriteset(task.AbsoluteIndex, task.Incarnation, task.TotalTask)
		mv.ClearReadset(task.AbsoluteIndex)
		mv.ClearIterateset(task.AbsoluteIndex)
	}
}

func (s *scheduler) start1(ctx context.Context, ch chan func(), workers int) {
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

func (s *scheduler) start2(ctx context.Context, ch chan func(), workers int) {
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

func (s *scheduler) start(ctx context.Context, ch chan func(), workers int) {
	for i := 0; i < workers; i++ {
		go func(worker int) {
			t0 := time.Now()
			for {
				select {
				case <-ctx.Done():
					return
				case work := <-ch:
					t1 := time.Now()
					work()
					fmt.Println("scheduler start", "waitTime", t1.Sub(t0).String(), "worker time", time.Since(t1).String())
					t0 = time.Now()

				}
			}
		}(i)
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

func (s *scheduler) DoWriteLatest(work func()) {
	s.writeLatest <- work
}

func (s *scheduler) DoExecute(work func()) {
	if s.synchronous {
		work()
		return
	}
	s.executeCh <- work
	//go work()
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

func (s *scheduler) toTasks(ctx sdk.Context, req sdk.DeliverTxBatchRequest) ([]*deliverTxTask, map[int]*deliverTxTask) {
	reqs := req.TxEntries
	orders := req.Orders
	tasksMap := make(map[int]*deliverTxTask)
	allTasks := make([]*deliverTxTask, 0, len(reqs))
	totalTask := len(reqs)
	interChainIndexes := make(map[int32]int32)
	txDepends := make([][]int, len(reqs), len(reqs))

	t0 := time.Now()
	if orders != nil {
		if orders.NsInterChainTransactionOrders != nil {
			for _, orderEntry := range orders.NsInterChainTransactionOrders.Entries {
				for i, idxEntry := range orderEntry.Indexes {
					if i > 0 {
						interChainIndexes[idxEntry.Index] = orderEntry.Indexes[i-1].Index
					}
				}
			}
		}

		if orders.NsTransactionOrders != nil {
			for _, orderEntry := range orders.NsTransactionOrders.Entries {
				for _, idxes := range orderEntry.Indexes {
					for i, idx := range idxes.Indexes {
						dependIdxes := make([]int, 0)
						if i > 0 {
							dependIdxes = append(dependIdxes, int(idxes.Indexes[i-1]))
						}
						interChainDependIdx, hasDep := interChainIndexes[idx]
						if hasDep {
							dependIdxes = append(dependIdxes, int(interChainDependIdx))
						}
						txDepends[idx] = dependIdxes
					}
				}
			}
		}
	}
	t1 := time.Now()

	wg := &sync.WaitGroup{}
	wg.Add(len(reqs))
	peek := s.moveExecutionCounterHandler(ctx, len(reqs))
	for idx, tx := range reqs {
		go func(idx int, callTx *sdk.DeliverTxEntry) {
			defer wg.Done()
			callTx.NewAccAddresses = s.txNewAccountAddressesHandler(ctx.WithNamepace(s.namespaceList[idx]), callTx.SdkTx)
		}(idx, tx)
	}
	wg.Wait()
	t2 := time.Now()

	for idx, tx := range reqs {
		accountNumbers := s.nextAccountNumbers(ctx, s.namespaceList[idx], tx.NewAccAddresses)
		task := &deliverTxTask{
			Request:          tx.Request,
			SdkTx:            tx.SdkTx,
			AbsoluteIndex:    tx.AbsoluteIndex,
			Status:           statusPending,
			Dependencies:     map[int]struct{}{},
			TotalTask:        totalTask,
			IsEVMTx:          false,
			AccountNumbers:   accountNumbers,
			ExecutionCounter: peek,
			//TxTracer:      r.TxTracer,
		}
		peek++

		// if there are dependencies, set the task to waiting
		dependIdxes := txDepends[idx]
		if len(dependIdxes) > 0 {
			dependencies := map[int]struct{}{}
			for _, dependIdx := range dependIdxes {
				dependencies[dependIdx] = struct{}{}
			}
			task.Status = statusWaiting
			task.Dependencies = dependencies
		}
		tasksMap[tx.AbsoluteIndex] = task
		allTasks = append(allTasks, task)
	}
	t3 := time.Now()
	println("to task time=======", "orders", t1.Sub(t0).String(), "new account address", t2.Sub(t1).String(), "next number", t3.Sub(t2).String())
	return allTasks, tasksMap
}
func (s *scheduler) nextAccountNumbers(ctx sdk.Context, namespaceId uint64, newAccAddresses []sdk.NamespaceAccount) map[string]map[uint64]uint64 {
	nextAccountNumbers := s.nextAccountNumberHandler(ctx.WithNamepace(namespaceId).WithNextAccountNumber(s.allNextAccountNumbers), newAccAddresses)
	if nextAccountNumbers != nil && len(nextAccountNumbers) > 0 {
		for addr, accNumbers := range nextAccountNumbers {
			for nsId, accNumber := range accNumbers {
				if _, ok := s.allNextAccountNumbers[addr]; !ok {
					s.allNextAccountNumbers[addr] = make(map[uint64]uint64)
				}
				s.allNextAccountNumbers[addr][nsId] = accNumber
			}
		}
	}
	return nextAccountNumbers
}

func (s *scheduler) collectResponses(ctx sdk.Context, tasks []*deliverTxTask) []abci.ExecTxResult {
	res := make([]abci.ExecTxResult, 0, len(tasks))
	logSizes := make(map[uint64]uint64)
	txLogses := make(map[uint64][]string)
	consumeGasWanted := make(map[uint64]int64)
	namespaceBlockTxIndexes := make(map[uint64]int)

	// start time
	startTime := time.Now()

	for _, t := range tasks {
		res = append(res, *t.Response)
		s.consumeGas(ctx, t)
		if gasWanted, ok := consumeGasWanted[t.NamespaceId]; ok {
			consumeGasWanted[t.NamespaceId] = gasWanted + t.Response.GasWanted
		} else {
			consumeGasWanted[t.NamespaceId] = t.Response.GasWanted
		}
		t.IsEVMTx = s.isEVMTxHandler(ctx, t.SdkTx)

		if !t.IsEVMTx {
			continue
		}
		evmTxIndex := 0
		if idx, ok := namespaceBlockTxIndexes[t.NamespaceId]; ok {
			evmTxIndex = idx
		} else {
			namespaceBlockTxIndexes[t.NamespaceId] = evmTxIndex
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

					logSize := uint64(0)
					if size, ok := logSizes[t.NamespaceId]; ok {
						logSize = size
					}
					txLogs := make([]string, 0)
					if logs, ok := txLogses[t.NamespaceId]; ok {
						txLogs = logs
					}

					newLog := s.modifyLogIndexHandler(ctx, e.Attributes[i].Value, logSize)
					e.Attributes[i].Value = newLog
					txLogs = append(txLogs, newLog)
					txLogses[t.NamespaceId] = txLogs
					logSizes[t.NamespaceId] = logSize + 1
				}
			}
		}
		//if t.Response.Code == 0 {
		//	namespaceBlockTxIndexes[t.NamespaceId] = evmTxIndex + 1
		//}
		namespaceBlockTxIndexes[t.NamespaceId] = evmTxIndex + 1

	}
	t1 := time.Now()
	s.consumeGasWanted(ctx, consumeGasWanted)
	t2 := time.Now()

	for namespaceId, txLogs := range txLogses {
		s.setBlockBloomTransientHandler(ctx.WithNamepace(namespaceId), txLogs)
	}
	// calculate time used
	println("========= CollectResponses ", "for time", t1.Sub(startTime).String(), "connsume gas wanted time", t2.Sub(t1).String(), "set block bloom time", time.Since(t2).String())
	//fmt.Printf("========= CollectResponses Time used: %v\n", time.Since(startTime))
	return res
}

func (s *scheduler) consumeGas(ctx sdk.Context, task *deliverTxTask) {
	taskCtx := task.Ctx

	limit := taskCtx.GasMeter().Limit()
	gasUsed := uint64(task.Response.GasUsed)
	consumed := storetypes.Gas(0)
	if gasUsed > limit {
		consumed = limit
	} else {
		if gasUsed > 0 {
			consumed = gasUsed
		}
	}

	ctx.BlockGasMeter().WithNamespaceId(task.NamespaceId).ConsumeGas(
		consumed, "block gas meter",
	)
}

func (s *scheduler) consumeGasWanted(ctx sdk.Context, consumeGasWanted map[uint64]int64) {
	for namespaceId, gasWanted := range consumeGasWanted {
		if gasWanted < 0 {
			gasWanted = 0
		}
		s.addTransientGasWanted(ctx.WithNamepace(namespaceId), uint64(gasWanted))
	}
}

func (s *scheduler) tryInitMultiVersionStore(ctx sdk.Context, totalTask int) {
	if s.multiVersionStores != nil {
		return
	}
	mvs := make(map[storetypes.StoreKey]multiversion.MultiVersionStore)
	keys := ctx.MultiStore().StoreKeys()
	listStore := os.Getenv("listStore")
	for _, sk := range keys {
		if listStore == "" {
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
	tasks, tasksMap := s.toTasks(ctx, req)
	println("init version store time========", "sub", toTaskStartTime.Sub(startTime).String(), "len(tasks)", len(tasks))

	fmt.Printf("========= toTasks Time used: %v\n", time.Since(toTaskStartTime))
	//now := time.Now()
	s.allTasks = tasks
	s.allTasksMap = tasksMap
	s.executeCh = make(chan func(), len(tasks))
	s.validateCh = make(chan func(), len(tasks))
	s.collectCh = make(chan func(), len(tasks))
	s.writeLatest = make(chan func(), len(tasks))
	//defer s.emitMetrics()

	// default to number of tasks if workers is negative or 0 by this point
	workers := s.workers
	if s.workers < 1 || len(tasks) < s.workers {
		workers = len(tasks)
	}

	workerCtx, cancel := context.WithCancel(ctx.Context())
	defer cancel()
	// set trace store file
	tracePathEnv := os.Getenv("tracePath")
	if tracePathEnv != "" {
		go printCpuCount()
	}

	t6 := time.Now()
	// execution tasks are limited by workers
	s.start(workerCtx, s.executeCh, workers)
	println("start worker ctx", "", time.Since(t6).String())

	s.start1(workerCtx, s.collectCh, workers)
	//t6 := time.Now()
	//println("======================", "sub", t6.Sub(now).String())

	toExecute := tasks
	//go printCpuCount()

	for !allValidated(tasks) {
		// if the max incarnation >= x, we should revert to synchronous
		if iterations >= maximumIterations || s.synchronous {
			// process synchronously
			trigger := s.synchronous
			s.synchronous = true
			startIdx, anyLeft := s.findFirstNonValidated()
			if !anyLeft {
				break
			}

			toExecute = tasks[startIdx:]
			if trigger {
				ctx.Logger().Warn("validate tasks trigger synchronous", "iterations", iterations, "maximumIterations", maximumIterations, "toExecute=", len(toExecute), "startIdx=", startIdx, "anyLeft=", anyLeft)
			}
		}

		//go printCpuCount()
		printSystemStats("start ")

		// execute sets statuses of tasks to either executed or aborted
		t1 := time.Now()
		if err := s.executeAll(ctx, toExecute); err != nil {
			return nil, err
		}
		t2 := time.Now()

		//writeNow := cmttime.Now()
		// write from version store to multiversion stores

		listStore := os.Getenv("listStore")
		totalExecuted := 0

		if listStore == "" && !s.synchronous {
			t := time.Now()
			var wg sync.WaitGroup
			for _, task := range tasks {
				if task.Status == statusExecuted {
					totalExecuted++
					wg.Add(1)
					go func(task *deliverTxTask) {
						defer wg.Done()
						for _, v := range task.VersionStores {
							for key := range v.Writeset {
								v.NewKey(key, task.TotalTask)
							}
						}
					}(task)
				}
			}
			wg.Wait()
			println("new key time =========================", "count time", time.Now().Sub(t).String(), "totalExecuted", totalExecuted)
		}

		t3 := time.Now()

		if listStore == "" && !s.synchronous {
			wg := &sync.WaitGroup{}
			wg.Add(totalExecuted)

			for _, task := range tasks {
				if task.IsStatus(statusExecuted) {
					writeTask := task
					s.DoWrite(func() {
						defer wg.Done()
						for _, v := range writeTask.VersionStores {
							v.WriteToMultiVersionStore()
						}

					})
				}
			}
			wg.Wait()
		}

		t4 := time.Now()
		s.startVal(workerCtx, s.validateCh, workers)

		var err error
		toExecute, err = s.validateAll(ctx, tasks)
		printSystemStats("end ")
		t5 := time.Now()
		if err != nil {
			return nil, err
		}
		// these are retries which apply to metrics
		s.metrics.retries += len(toExecute)

		println("=========", "executeAll time", t2.Sub(t1).String(), "new key", t3.Sub(t2).String(), "write version store", t4.Sub(t3).String(), "validateAll", t5.Sub(t4).String(), "iterations", iterations)
		iterations++
	}
	t2 := time.Now()
	s.start2(workerCtx, s.writeLatest, workers)

	wg := &sync.WaitGroup{}

	for _, mv := range s.multiVersionStores {
		tmp := mv
		wg.Add(1)
		s.DoWriteLatest(func() {
			defer wg.Done()
			tmp.WriteLatestToStore()
		})
	}
	wg.Wait()
	t3 := time.Now()
	println("write latest to store", "sub", t3.Sub(t2).String(), "start total sub", t2.Sub(startTime).String())

	// end trace
	s.metrics.maxIncarnation = s.maxIncarnation

	res := s.collectResponses(ctx, tasks)
	//printSystemStats("after process")

	//s.setMsgsHandler(ctx, evmTxs)
	ctx.Logger().Info("occ scheduler", "height", ctx.BlockHeight(), "txs", len(tasks), "latency_ms", time.Since(startTime).String(), "retries", s.metrics.retries, "maxIncarnation", s.maxIncarnation, "iterations", iterations, "sync", s.synchronous, "workers", workers)

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

func (s *scheduler) validateAll(ctx sdk.Context, tasks []*deliverTxTask) ([]*deliverTxTask, error) {
	//ctx, span := s.traceSpan(ctx, "SchedulerValidateAll", nil)
	//defer span.End()

	//var mx sync.Mutex

	startIdx, anyLeft := s.findFirstNonValidated()

	if !anyLeft {
		return nil, nil
	}

	res := make([]*deliverTxTask, len(tasks))
	wg := &sync.WaitGroup{}

	for i := startIdx; i < len(tasks); i++ {
		wg.Add(1)
		t := tasks[i]
		s.DoValidate(func() {
			defer wg.Done()
			if !s.validateTask(ctx, t) {
				//mx.Lock()
				//defer mx.Unlock()
				t.Reset()
				t.Increment()
				// update max incarnation for scheduler
				if t.Incarnation > s.maxIncarnation {
					s.maxIncarnation = t.Incarnation
				}
				res[i] = t
			}
		})
	}
	//printSystemStats("validating")

	wg.Wait()
	ret := make([]*deliverTxTask, 0, len(res))
	for i := 0; i < len(res); i++ {
		if res[i] != nil {
			ret = append(ret, res[i])
		}

	}

	return ret, nil
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

	now := time.Now()
	for _, task := range tasks {
		if !s.synchronous && task.IsStatus(statusWaiting) {
			continue
		}
		t := task
		wg.Add(1)
		s.DoExecute(func() {
			defer wg.Done()
			//now := time.Now()
			sdkCtx := ctx.WithNamepace(t.NamespaceId).WithNextAccountNumber(t.AccountNumbers).WithSynchronous(s.synchronous).WithTxIndex(task.AbsoluteIndex).WithExecutionCounter(task.ExecutionCounter)
			s.prepareAndRunTask(wg, sdkCtx, t)
			//println("run task time", "sub", time.Now().Sub(now).String())
		})
	}
	//printSystemStats("executing")
	println("do execute to task", "", time.Since(now).String(), "task length", len(tasks), "executeCh length", len(s.executeCh))
	wg.Wait()

	return nil
}

func (s *scheduler) findFirstNonValidated() (int, bool) {
	for i, t := range s.allTasks {
		if t.Status != statusValidated {
			return i, true
		}
	}
	return 0, false
}
func (s *scheduler) prepareAndRunTask(wg *sync.WaitGroup, ctx sdk.Context, task *deliverTxTask) {
	//eCtx, eSpan := s.traceSpan(ctx, "SchedulerExecute", task)
	//defer eSpan.End()
	//now := time.Now()

	//C.printThreadID() // 打印线程ID
	//now := cmttime.Now()
	task.Ctx = ctx
	s.executeTask(task)
	listStore := os.Getenv("listStore")
	if listStore == "" {
		if s.synchronous {
			for _, v := range task.VersionStores {
				writeset := v.GetWriteset()
				for key, _ := range writeset {
					v.NewKey(key, task.TotalTask)
				}
				v.WriteToMultiVersionStore()
			}
		}
	}

	//println("run tast time", "sub", time.Now().Sub(now).String())

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
	t1 := time.Now()
	ctx := task.Ctx.WithTxIndex(task.AbsoluteIndex)

	//_, span := s.traceSpan(ctx, "SchedulerPrepare", task)
	//defer span.End()

	// initialize the context
	abortCh := make(chan occ.Abort, len(s.multiVersionStores))
	t2 := time.Now()
	// print len(s.multiVersionStores)

	var t3, t4 time.Time
	// if there are no stores, don't try to wrap, because there's nothing to wrap
	if len(s.multiVersionStores) > 0 {
		// non-blocking
		cms := ctx.MultiStore().CacheMultiStore()

		// init version stores by store key
		vs := make(map[storetypes.StoreKey]*multiversion.VersionIndexedStore)
		for storeKey, mvs := range s.multiVersionStores {
			vs[storeKey] = mvs.VersionedIndexedStore(task.AbsoluteIndex, task.Incarnation, abortCh, task.TotalTask)
		}
		t3 = time.Now()

		// save off version store so we can ask it things later
		task.VersionStores = vs
		ms := cms.SetKVStores(func(k storetypes.StoreKey, kvs storetypes.KVStore) store.CacheWrap {
			return vs[k]
		})

		t4 = time.Now()

		ctx = ctx.WithMultiStore(ms)
		namespaceId := s.namespaceList[task.AbsoluteIndex]
		ctx = ctx.WithNamepace(namespaceId)
		task.NamespaceId = namespaceId
	}

	task.AbortCh = abortCh
	task.Ctx = ctx
	t5 := time.Now()
	fmt.Sprintf("prepareTask", "make chan", t2.Sub(t1).String(), "range mvs", t3.Sub(t2).String(), "set kvstores", t4.Sub(t3).String(), "namespaceId", t5.Sub(t4).String(), "mvs len", len(s.multiVersionStores))
}

func (s *scheduler) executeTask(task *deliverTxTask) {
	//dCtx, dSpan := s.traceSpan(task.Ctx, "SchedulerExecuteTask", task)
	//defer dSpan.End()
	//task.Ctx = dCtx
	startTime := time.Now()
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
	t1 := time.Now()

	s.prepareTask(task)
	//prepareTaskSleepDuration := cmttime.Now().Sub(prepareTaskNow)
	//println("prepareTaskNow task: ", task.AbsoluteIndex, " time: ", time.Now().Sub(prepareTaskNow).String())
	t2 := time.Now()

	//now := cmttime.Now()
	//t0 := time.Now()
	//now := cmttime.Now()
	resp := s.deliverTx(task.Request, task.Ctx)
	//t1 := time.Now()
	//println("deliver tx=============", "sppend", t1.Sub(t0).String()) //sleepDuration := cmttime.Now().Sub(now)
	//println("deliverTx task: ", task.AbsoluteIndex, " time: ", sleepDuration, " from: ", now.String())
	t3 := time.Now()
	// close the abort channel
	close(task.AbortCh)
	abort, ok := <-task.AbortCh
	t4 := time.Now()
	//estimateNow := cmttime.Now()
	if ok {
		// if there is an abort item that means we need to wait on the dependent tx
		task.SetStatus(statusAborted)
		task.Abort = &abort
		task.AppendDependencies([]int{abort.DependentTxIdx})
		// write from version store to multiversion stores
		for _, v := range task.VersionStores {
			v.WriteEstimatesToMultiVersionStore()
		}
		return
	}
	t5 := time.Now()

	if resp.Code == errors.ErrOCCAbort.ABCICode() {
		task.SetStatus(statusAborted)
		s.synchronous = true
		return
	}

	//estimateSleepDuration := cmttime.Now().Sub(estimateNow)
	//println("EstimateSleepDuration task: ", task.AbsoluteIndex, " time: ", estimateSleepDuration, " from: ", estimateNow.String())

	task.SetStatus(statusExecuted)
	task.Response = resp

	t6 := time.Now()
	//writeNow := cmttime.Now()
	//// write from version store to multiversion stores
	//for _, v := range task.VersionStores {
	//	v.WriteToMultiVersionStore()
	//}
	listStore := os.Getenv("listStore")
	if listStore != "" {
		for _, v := range task.VersionStores {
			v.WriteToMultiVersionStore()
		}
	}
	t7 := time.Now()

	fmt.Sprintf("============executeTask",
		"synchronous", t1.Sub(startTime).String(),
		"prepare", t2.Sub(t1).String(),
		"deliverTx", t3.Sub(t2).String(),
		"abortChClose", t4.Sub(t3).String(),
		"abortOk", t5.Sub(t4).String(),
		"setStatus", t6.Sub(t5).String(),
		"writeStore", t7.Sub(t6).String(),
	)
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
		time.Sleep(50 * time.Millisecond)
	}

}

func printSystemStats(desc string) {
	//fmt.Printf("Stage: %s\n", desc)
	//var m runtime.MemStats
	//runtime.ReadMemStats(&m)
	//fmt.Printf("Goroutines: %d\n", runtime.NumGoroutine())
	//fmt.Printf("CPUs: %d\n", runtime.NumCPU())
	//fmt.Printf("GOMAXPROCS: %d\n", runtime.GOMAXPROCS(0))
	//fmt.Printf("Threads: %d\n", threadProfile.Count())
	//fmt.Printf("Memory Alloc = %v MiB\n", m.Alloc/1024/1024)
	//fmt.Printf("TotalAlloc = %v MiB\n", m.TotalAlloc/1024/1024)
	//fmt.Printf("Sys = %v MiB\n", m.Sys/1024/1024)
	//fmt.Printf("NumGC = %v\n", m.NumGC)
	//fmt.Printf("HeapAlloc: %v MB\n", m.HeapAlloc/1024/1024) // 堆上实际使用的内存
	//fmt.Printf("HeapSys: %v MB\n", m.HeapSys/1024/1024)     // 系统分配给堆的内存
	//fmt.Printf("HeapObjects: %v\n", m.HeapObjects)          // 堆上分配的对象数量

	//printCpu()
}

func printCpu() {

	percent, err := cpu.Percent(time.Millisecond*10, false)
	if err != nil {
		fmt.Println(err)
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
