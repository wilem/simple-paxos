package main

import (
	"testing"
	"time"
	//"fmt"
)

func TestNode(t *testing.T) {
	/*
		node1 := NewNode(10)
		node1.Start()
		node2 := NewNode(20)
		node2.Start()

		node1.SendTo(20, []byte("1234567890 - 1 -> 2"))

		time.Sleep(time.Second)
	*/

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

	time.Sleep(time.Second)
}
