package main

import (
	"log"
	"testing"
	"time"
	//"fmt"
)

func Test2Nodes(t *testing.T) {
	n1 := NewNodeLoad("node1.cfg")
	n2 := NewNodeLoad("node2.cfg")
	n1.Start()
	n2.Start()
	n, err := n1.SendTo(2, []byte("xxx,foo"))
	if n == 0 || err != nil {
		t.Errorf("n1.SendTo:%s\n", err)
	}

	n, err = n2.SendTo(1, []byte("xxx,bar"))
	if n == 0 || err != nil {
		t.Errorf("n2.SendTo:%s\n", err)
	}
	for i := 0; i < 2; i++ {
		time.Sleep(time.Second)
	}
}

func TestNode(t *testing.T) {
	n1 := NewNodeLoad("node1.cfg")
	n2 := NewNodeLoad("node2.cfg")
	n3 := NewNodeLoad("node3.cfg")
	n9 := NewNodeLoad("node9.cfg")
	n1.Start()
	n2.Start()
	n3.Start()
	n9.Start()

	//client send multiple values.
	var seq uint32
	var val Value
	const rnd = 10
	for {
		seq++
		val.siz = 4
		val.oct = make([]byte, 4)
		val.oct[0] = byte(seq % 255)
		ret, err := n9.client.Submit(seq, &val)
		if err != nil {
			log.Println("Submit failed - ret,err =", ret, err)
		}
		time.Sleep(time.Second)
		if seq == rnd {
			break
		}
	}

	for i := 0; i < rnd+1; i++ {
		time.Sleep(time.Second)
	}
}
