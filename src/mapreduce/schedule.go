package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	idleworker := make(chan string)
	jobs := make(chan int)
	doneworks := make(chan bool)
	go func() {
		for {
			newRegister := <- mr.registerChannel
			idleworker <- newRegister
		}
	}()

	go func() {
		for _, w := range mr.workers{
			idleworker <- w
		}
	}()

	go func() {
		for i := 0; i < ntasks; i++ {
			jobs <- i
		}
	}()

	go func() {
		for idx := range jobs {
			availableworker := <- idleworker
			go func(idx int, availableworker string) {
				debug("worker: %s", availableworker)
				args := new(DoTaskArgs)
				args.JobName = mr.jobName
				args.TaskNumber = idx
				args.NumOtherPhase = nios
				args.Phase = phase
				if phase == mapPhase {
					args.File = mr.files[idx]
				}
				ok := call(availableworker, "Worker.DoTask", args, new(struct{}))
				if ok == true {
					doneworks <- true
					idleworker <- availableworker
				} else {
					jobs <- idx
				}
			}(idx, availableworker)
		}
	}()

	for i := 0; i < ntasks; i++ {
		<- doneworks
	}
	close(jobs)
	fmt.Printf("Schedule: %v phase done\n", phase)
}
