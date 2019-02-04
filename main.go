package pipeline

import (
	"reflect"
	"sync"
)

type Layer []interface{}

type Pipeline struct {
	line []Layer
}

type Source func(chan <- interface{})
type Sink   func(<- chan interface{})
type Pipe   func(in <- chan interface{}, out chan <- interface{})

func NewPipeLine(source Source, n int) *Pipeline {
	ints := make([]interface{}, n)

	for i := 0; i < n; i++ {
		ints[i] = source
	}

	return &Pipeline{
		line: []Layer{ints},
	}
}

func (p *Pipeline) WithPipe(pipe Pipe, n int) *Pipeline {
	layer := make([]interface{}, n)
	for i := 0; i < n; i++ {
		layer[i] = pipe
	}

	p.line = append(p.line, layer)
	return p
}

func (p *Pipeline) WithSinks(sink Sink, n int) *Pipeline {
	ints := make([]interface{}, n)
	for i := 0; i < n; i++ {
		ints[i] = sink
	}

	p.line = append(p.line, ints)
	return p
}

func mux(ins []chan interface{}, outs []chan interface{}) {
	closed := make([]bool, len(ins), len(ins))
	nClosed := 0

	for {
		if nClosed == len(ins) {
			// if all input channels are closed, close all outputs and return
			for i := range outs {
				close(outs[i])
			}
			return
		}

		// read from some input
		cases := make([]reflect.SelectCase, 0)
		caseInds := make([]int, 0, 0)
		for i := 0; i < len(ins); i++ {
			if closed[i] {
				continue
			}

			cases = append(cases, reflect.SelectCase{
				Dir: reflect.SelectRecv,
				Chan: reflect.ValueOf(ins[i]),
			})
			caseInds = append(caseInds, i)
		}
		chosen, recv, _ := reflect.Select(cases)
		chosen = caseInds[chosen]
		if recv.IsNil() {
			closed[chosen] = true
			nClosed++
			continue
		}

		// process the data (feed into some output)
		cases = make([]reflect.SelectCase, len(outs))
		for i := 0; i < len(outs); i++ {
			cases[i] = reflect.SelectCase{
				Dir: reflect.SelectSend,
				Chan: reflect.ValueOf(outs[i]),
				Send: recv,
			}
		}

		reflect.Select(cases)
	}
}


func makeChans(len int) []chan interface{} {
	res := make([]chan interface{}, len)
	for i := 0; i < len; i++ {
		res[i] = make(chan interface{}, 1)
	}

	return res
}

func (p *Pipeline) Up() *sync.WaitGroup {
	sourceChans := makeChans(len(p.line[0]))

	for i, sch := range sourceChans {
		go p.line[0][i].(Source)(sch)
	}

	insFirst := sourceChans

	for i := 1; i < len(p.line) - 1; i++ {
		insSecond := makeChans(len(p.line[i]))
		go mux(insFirst, insSecond)

		outsFirst := makeChans(len(p.line[i]))

		for j, fn := range p.line[i] {
			go fn.(Pipe)(insSecond[j], outsFirst[j])
		}

		insFirst = outsFirst
	}

	// last part -- sinks
	wg := &sync.WaitGroup{}
	insSecond := makeChans(len(p.line[len(p.line) - 1]))
	go mux(insFirst, insSecond)
	wg.Add(len(p.line[len(p.line) - 1]))

	for j, fn := range p.line[len(p.line) - 1] {
		go func(w *sync.WaitGroup, f Sink, ch <- chan interface{}) {
			defer w.Done()

			f(ch)
		}(wg, fn.(Sink), insSecond[j])
	}

	return wg
}

func PipeFromFunc(f func(interface{})interface{}) Pipe {
	return func(in <- chan interface{}, out chan <- interface{}) {
		for i := range in {
			out <- f(i)
		}
		close(out)
	}
}