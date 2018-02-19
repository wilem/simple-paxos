package main

import (
	"fmt"
	"testing"
	"time"
)

func TestUDPTranport(t *testing.T) {
	var u1, u2 *UDPTransport
	u1 = NewUDPTransport(1)
	u1.OnRecv = OnRecv
	//XXX set cb before Start server loop
	e := u1.Start()
	if e != nil {
		t.Errorf("Start failed:%s\n", e)
	}

	u2 = NewUDPTransport(2)
	u2.OnRecv = OnRecv
	e = u2.Start()
	if e != nil {
		t.Errorf("Start failed:%s\n", e)
	}

	var n int
	str1 := "hello,u2!000"
	dat1 := []byte(str1)
	n, e = u1.SendTo(2, dat1)
	if n != len(dat1) || e != nil {
		t.Errorf("SendTo failed: n:%d(%d),e:%s\n", n, len(dat1), e)
	}

	str2 := "hello,u1!000"
	dat2 := []byte(str2)
	n, e = u2.SendTo(1, dat2)
	if n != len(dat2) || e != nil {
		t.Errorf("SendTo failed: n:%d(%d),e:%s\n", n, len(dat2), e)
	}

	//wait for server to exit.
	time.Sleep(time.Second * 2)

	ss1, ok1 := recvStrs[1]
	ss2, ok2 := recvStrs[2]
	if ok1 && ok2 && ss1 == str1 && ss2 == str2 {
		fmt.Println("recv matched.")
	} else {
		t.Errorf("msg mismatch - recv'ed ss1:%s, ss2:%s\n", ss1, ss2)
	}
}

var recvStrs map[uint32]string

func OnRecv(id uint32, dat []byte) {
	fmt.Printf("[%d]OnRecv - from:%d, data:%+v\n", 0, id, dat)
	if recvStrs == nil {
		recvStrs = make(map[uint32]string)
	}
	recvStrs[id] = string(dat)

}
