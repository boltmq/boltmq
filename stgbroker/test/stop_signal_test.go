package test

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
)

func TestStopSignal(t *testing.T) {
	var stopLock sync.Mutex
	stopChan := make(chan struct{}, 1)
	signalChan := make(chan os.Signal, 1)
	fmt.Println("开始...")

	signal.Notify(signalChan, os.Interrupt, os.Kill, syscall.SIGINT, syscall.SIGTERM)
	//模拟一个持续运行的进程

	go func() {
		//阻塞程序运行，直到收到终止的信号
		<-signalChan
		stopLock.Lock()
		fmt.Println("Cleaning before stop...")
		stopChan <- struct{}{}
		stopLock.Unlock()
	}()

	select {
	case <-stopChan:
		fmt.Println("结束!")
		os.Exit(0)
	}

}
