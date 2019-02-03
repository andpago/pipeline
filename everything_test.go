package pipeline

import (
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func Generator(out chan <- interface{}) {
	for i := 0; i < 1000; i++ {
		out <- rand.Intn(10)
	}

	close(out)
}

func Mult(in <- chan interface{}, out chan <- interface{}) {
	for i := range in {
		out <- i.(int) * 10
	}
	close(out)
}

func Plus(in <- chan interface{}, out chan <- interface{}) {
	for i := range in {
		out <- i.(int) + 1
	}
	close(out)
}

func Printer(in <- chan interface{}) {
	for i := range in {
		log.Println(i)
	}
}

func TestWorks(t *testing.T) {
	p := NewPipeLine(Generator, 1).WithPipe(Mult, 1).WithPipe(Plus, 1).WithSinks(Printer, 1)
	p.Up()
}

func TestWaits(t *testing.T) {
	cnt := 0
	mu := &sync.Mutex{}
	Counter := func (in <- chan interface{}) {
		for range in {
			mu.Lock()
			cnt++
			mu.Unlock()
		}
	}

	NewPipeLine(Generator, 1).WithPipe(PipeFromFunc(func(arg interface{})interface{}{
		time.Sleep(100 * time.Millisecond)
		return arg
	}), 100).WithSinks(Counter, 1).Up()


	if cnt != 1000 {
		t.Fatalf("cnt != 1000: %v", cnt)
	}
}