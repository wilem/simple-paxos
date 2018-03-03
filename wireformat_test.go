package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"testing"
)

func TestBytesBuffer(t *testing.T) {
	var buf bytes.Buffer
	var rd io.Reader
	var num uint32
	//feed buffer
	oct := []byte{0xFF, 0xFF, 0, 0}
	buf.Write(oct)
	rd = &buf //buffer to reader
	if err := binary.Read(rd, binary.LittleEndian, &num); err != nil {
		t.Error("binary.Read fail")
	}
	log.Println("num:", num)
	log.Println("buf:len,Bytes:", buf.Len(), buf.Bytes())
	buf.Reset()
}

func TestWireformat(t *testing.T) {
	//serialize/deserialize test for all PxsMsg.
	var buffer bytes.Buffer
	var rd io.Reader = &buffer
	//0.Test value
	{
		var val Value
		val.siz = 4
		oct := []byte{42, 84, 42, 84}
		val.oct = oct[0:val.siz]
		fld := []interface{}{&val.siz, &val.oct}
		bs0, err := serialize(fld)
		if err != nil {
			t.Error("m1 encode:", err)
		}
		log.Println("val,bs0=", val, bs0)
		//deser
		var val2 Value
		buffer.Write(bs0)
		//siz
		fld2 := []interface{}{&val2.siz}
		err = deserialize(fld2, rd)
		if err != nil {
			log.Println("deser - err:", err)
		}
		log.Printf("deser - val2:%+v\n", val2)
		//oct
		val2.oct = make([]byte, val2.siz)
		fld2 = []interface{}{&val2.oct}
		err = deserialize(fld2, rd)
		if err != nil {
			log.Println("deser - err:", err)
		}
		log.Printf("deser - val2:%+v\n", val2)
	}
	{
		//1.Request
		var v = &Value{4, []byte{101, 0, 0, 0}}
		var s = uint32(100)
		m1 := NewPxsMsgRequest(s, v)
		bs1, err := m1.Encode()
		if bs1 == nil || err != nil {
			t.Error("m1 encode:", err)
		}
		msg, hdr, rem, err := DecodeOnePxsMsg(&buffer, bs1)
		m2, ok := msg.(*PxsMsgRequest)
		if err != nil || !ok || rem != 0 || hdr == nil {
			t.Error("m2 decode: err,ok,rem,bs1,msg=", err, ok, rem, bs1, msg)
		}
		bs2, _ := m2.Encode()
		if !bytes.Equal(bs1, bs2) {
			t.Error("m1, m2 mismatch, m1:", m1, "m2:", m2)
		}
		log.Printf("m1:%+v,bs1:%+v\n", m1, bs1)
		log.Printf("m2:%+v,bs2:%+v\n", m1, bs2)
	}
	{
		//2.Prepare - P1a msg
		m1 := NewPxsMsgPrepare(101, 101)
		bs1, _ := m1.Encode()
		msg, _, rem, err := DecodeOnePxsMsg(&buffer, bs1)
		m2, ok := msg.(*PxsMsgPrepare)
		if err != nil || !ok || rem != 0 {
			t.Error("m2 decode:", err, ok, rem)
		}
		bs2, _ := m2.Encode()
		if !bytes.Equal(bs1, bs2) {
			t.Error("m1, m2 mismatch, m1:", m1, "m2:", m2)
		}
		log.Printf("m1:%+v,bs1:%+v\n", m1, bs1)
		log.Printf("m2:%+v,bs2:%+v\n", m1, bs2)
	}
	{
		//3.Promise - P1b msg
		v := new(Value)
		v.siz = 4
		v.oct = make([]byte, 4)
		v.oct[0] = 42
		m1 := NewPxsMsgPromise(1, 1, 101, 100, v)
		bs1, _ := m1.Encode()
		log.Println("Promise - bs1:", bs1)
		msg, _, rem, err := DecodeOnePxsMsg(&buffer, bs1)
		m2, ok := msg.(*PxsMsgPromise)
		if err != nil || !ok || rem != 0 {
			t.Error("m2 decode:", err, ok, rem, bs1)
		}
		log.Println("Promise - m2:", m2) //XXX
		bs2, _ := m2.Encode()
		if !bytes.Equal(bs1, bs2) {
			t.Error("m1, m2 mismatch, m1:", m1, "m2:", m2)
		}
		log.Printf("m1:%+v,bs1:%+v\n", m1, bs1)
		log.Printf("m2:%+v,bs2:%+v\n", m1, bs2)
	}
	{
		//4.Accept - P2a msg
		v := new(Value)
		v.siz = 4
		v.oct = make([]byte, 4)
		v.oct = []byte{42, 42, 42, 42}
		m1 := NewPxsMsgAccept(1, 101, v)
		bs1, _ := m1.Encode()
		log.Println("Accept - bs1:", bs1)
		msg, _, rem, err := DecodeOnePxsMsg(&buffer, bs1)
		m2, ok := msg.(*PxsMsgAccept)
		if err != nil || !ok || rem != 0 {
			t.Error("m2 decode:", err, ok, rem, bs1)
		}
		log.Println("Accept - m2:", m2)
		bs2, _ := m2.Encode()
		if !bytes.Equal(bs1, bs2) {
			t.Error("m1, m2 mismatch, m1:", m1, "m2:", m2)
		}
		log.Printf("m1:%+v,bs1:%+v\n", m1, bs1)
		log.Printf("m2:%+v,bs2:%+v\n", m1, bs2)
	}
	{
		//5.Accepted - P2b msg
		v := new(Value)
		v.siz = 4
		v.oct = make([]byte, 4)
		v.oct = []byte{42, 42, 42, 42}
		m1 := NewPxsMsgAccepted(1, 1, 101, v)
		bs1, _ := m1.Encode()
		log.Println("Accepted - bs1:", bs1)
		msg, _, rem, err := DecodeOnePxsMsg(&buffer, bs1)
		m2, ok := msg.(*PxsMsgAccepted)
		if err != nil || !ok || rem != 0 {
			t.Error("m2 decode:", err, ok, rem, bs1)
		}
		log.Println("Accept - m2:", m2)
		bs2, _ := m2.Encode()
		if !bytes.Equal(bs1, bs2) {
			t.Error("m1, m2 mismatch, m1:", m1, "m2:", m2)
		}
		log.Printf("m1:%+v,bs1:%+v\n", m1, bs1)
		log.Printf("m2:%+v,bs2:%+v\n", m1, bs2)
	}
	{
		//6.Commit - P3a msg
		m1 := NewPxsMsgCommit(1, 101)
		bs1, _ := m1.Encode()
		log.Println("Accepted - bs1:", bs1)
		msg, _, rem, err := DecodeOnePxsMsg(&buffer, bs1)
		m2, ok := msg.(*PxsMsgCommit)
		if err != nil || !ok || rem != 0 {
			t.Error("m2 decode:", err, ok, rem, bs1)
		}
		log.Println("Commit - m2:", m2)
		bs2, _ := m2.Encode()
		if !bytes.Equal(bs1, bs2) {
			t.Error("m1, m2 mismatch, m1:", m1, "m2:", m2)
		}
		log.Printf("m1:%+v,bs1:%+v\n", m1, bs1)
		log.Printf("m2:%+v,bs2:%+v\n", m1, bs2)
	}
	{
		//7.Response - P0b msg
		m1 := NewPxsMsgResponse(1, 0)
		bs1, _ := m1.Encode()
		log.Println("Response - bs1:", bs1)
		msg, _, rem, err := DecodeOnePxsMsg(&buffer, bs1)
		m2, ok := msg.(*PxsMsgResponse)
		if err != nil || !ok || rem != 0 {
			t.Error("m2 decode:", err, ok, rem, bs1)
		}
		log.Println("Response - m2:", m2)
		bs2, _ := m2.Encode()
		if !bytes.Equal(bs1, bs2) {
			t.Error("m1, m2 mismatch, m1:", m1, "m2:", m2)
		}
		log.Printf("m1:%+v,bs1:%+v\n", m1, bs1)
		log.Printf("m2:%+v,bs2:%+v\n", m1, bs2)
	}
}
