package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
)

//IPxsMsg : generic pxs msg type
type IPxsMsg interface {
	Encode() ([]byte, error)
	//Decode([]byte) error
}

//PxsMsgType : type field
type PxsMsgType uint32

//all pxs msg type
const (
	PxsMsgTypeRequest  PxsMsgType = 0x0a //0a msg: pro -> cli
	PxsMsgTypePrepare  PxsMsgType = 0x1a //1a msg: pro -> acc
	PxsMsgTypePromise  PxsMsgType = 0x1b //1b msg: acc -> pro
	PxsMsgTypeAccept   PxsMsgType = 0x2a //2a msg: pro -> acc
	PxsMsgTypeAccepted PxsMsgType = 0x2b //2b msg: acc -> pro
	PxsMsgTypeCommit   PxsMsgType = 0x3a //3a msg: pro -> acc
	PxsMsgTypeResponse PxsMsgType = 0x0b //0b msg: pro -> cli
)

//PxsMsgHeader of all pxs msg
type PxsMsgHeader struct {
	Len uint32     //msg length
	Typ PxsMsgType //msg type ID: 0a,1a,1b,2a,2b,3a,0b
	Iid uint32     //instance ID or Sequence num of request.
}

//Value : Client value, size < 64K
type Value struct {
	Siz uint32 //0: Value is none.
	Oct []byte //if oct == nil, then val == num;
}

//Size : len of bytes
func (v Value) Size() uint32 {
	return uint32(len(v.Oct))
}

//IsNone : v is a None Value.
func (v Value) IsNone() bool {
	if v.Siz == 0 || len(v.Oct) == 0 {
		return true
	}
	return false
}

//////////////////////////////////////////////////////////////////////////////////

//PxsMsgRequest :
type PxsMsgRequest struct {
	Hdr PxsMsgHeader // hdr.type = PxsMsgTypeRequest, hdr.iid as seq num of value;
	Val Value
}

//NewPxsMsgRequest :  new msg
func NewPxsMsgRequest(seq uint32, val *Value) *PxsMsgRequest {
	m := new(PxsMsgRequest)
	m.Hdr = PxsMsgHeader{
		Len: 4 + val.Size(), //Val.Siz + Val.Oct
		Iid: seq,
		Typ: PxsMsgTypeRequest,
	}
	m.Val = *val
	return m
}

//serialize :
func serialize(data []interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	for _, v := range data {
		err := binary.Write(buf, binary.LittleEndian, v)
		//TODO assert
		if err != nil {
			log.Println("binary.Wrile failed:", err)
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

//deserialize : element of flds should be pointer to filed of struct.
func deserialize(flds []interface{}, r io.Reader) error {
	for _, v := range flds {
		if err := binary.Read(r, binary.LittleEndian, v); err != nil {
			log.Println("deserialize failed, err:", err)
			return err
		}
	}
	return nil
}

//Encode : struct to bytes
func (m PxsMsgRequest) Encode() ([]byte, error) {
	var data = []interface{}{
		m.Hdr, m.Val.Siz, m.Val.Oct,
	}
	return serialize(data)
}

//PxsMsgResponse : P0b msg
type PxsMsgResponse struct {
	Hdr PxsMsgHeader // hdr.type = PxsMsgResponse, hdr.iid as seq num of value;
	Ret uint32       // return code; OK or TIMEOUT;
}

//NewPxsMsgResponse :
func NewPxsMsgResponse(iid, ret uint32) *PxsMsgResponse {
	m := new(PxsMsgResponse)
	m.Ret = ret
	m.Hdr = PxsMsgHeader{
		Len: 4, Typ: PxsMsgTypeResponse, Iid: iid,
	}
	return m
}

//Encode : struct to bytes
func (m PxsMsgResponse) Encode() ([]byte, error) {
	var data = []interface{}{
		m.Hdr, //header
		m.Ret,
	}
	return serialize(data)
}

//PxsMsgPrepare :
type PxsMsgPrepare struct {
	Hdr PxsMsgHeader
	Bal uint32
}

//InvalidBallot : None value;
const InvalidBallot = uint32(0xFFFFFFFF)

//NewPxsMsgPrepare :
func NewPxsMsgPrepare(iid, bal uint32) *PxsMsgPrepare {
	m := new(PxsMsgPrepare)
	m.Hdr = PxsMsgHeader{
		Len: 4, Iid: iid, Typ: PxsMsgTypePrepare,
	}
	m.Bal = bal
	return m
}

//Encode : struct to bytes
func (m PxsMsgPrepare) Encode() ([]byte, error) {
	var data = []interface{}{
		m.Hdr, //header
		m.Bal,
	}
	return serialize(data)
}

//PxsMsgPromise :
type PxsMsgPromise struct {
	hdr   PxsMsgHeader
	acc   uint32 //acceptor ID;
	bal   uint32 //bal > mBal;
	mVBal uint32 //voted ballot: if mBal != InvalidBallot then mVal is an old value;
	mVal  Value  //voted value: old value replied;
}

//NewPxsMsgPromise :
func NewPxsMsgPromise(iid, acc, bal, mVBal uint32, val *Value) *PxsMsgPromise {
	m := new(PxsMsgPromise)
	m.hdr = PxsMsgHeader{
		Len: 4*4 + val.Siz, //acc,bal,mvbal, siz, oct
		Typ: PxsMsgTypePromise,
		Iid: iid,
	}
	m.acc = acc
	m.bal = bal
	m.mVBal = mVBal
	m.mVal = *val
	return m
}

//Encode :
func (m PxsMsgPromise) Encode() ([]byte, error) {
	var data = []interface{}{
		m.hdr, //header
		m.acc, m.bal, m.mVBal,
		m.mVal.Siz, m.mVal.Oct,
	}
	return serialize(data)
}

//acceptors state:
//maxBal[a]  : promised ballot
//maxVBal[a] : accepted ballot
//maxVal[a]  : accepted value

//PxsMsgAccept :
type PxsMsgAccept struct {
	hdr PxsMsgHeader
	bal uint32
	val Value
}

//NewPxsMsgAccept :
func NewPxsMsgAccept(iid, bal uint32, val *Value) *PxsMsgAccept {
	m := new(PxsMsgAccept)
	m.bal = bal
	m.val = *val
	m.hdr = PxsMsgHeader{
		Len: 4 + 4 + m.val.Siz, //bal,val.Siz,val.Oct
		Typ: PxsMsgTypeAccept,
		Iid: iid,
	}
	return m
}

//Encode :
func (m PxsMsgAccept) Encode() (bs []byte, err error) {
	var data = []interface{}{
		m.hdr, //header
		m.bal, m.val.Siz, m.val.Oct,
	}
	return serialize(data)
}

//PxsMsgAccepted :
type PxsMsgAccepted struct {
	hdr PxsMsgHeader
	acc uint32
	bal uint32
	val Value
}

//NewPxsMsgAccepted :
func NewPxsMsgAccepted(iid, acc, bal uint32, val *Value) *PxsMsgAccepted {
	m := new(PxsMsgAccepted)
	m.acc, m.bal, m.val = acc, bal, *val
	m.hdr = PxsMsgHeader{
		Len: 4*3 + m.val.Siz, //acc,bal,val.Siz, val.Oct
		Typ: PxsMsgTypeAccepted,
		Iid: iid,
	}
	return m
}

//Encode :
func (m PxsMsgAccepted) Encode() ([]byte, error) {
	var data = []interface{}{
		m.hdr, //header
		m.acc, m.bal, m.val.Siz,
		m.val.Oct,
	}
	return serialize(data)
}

//PxsMsgCommit :
type PxsMsgCommit struct {
	hdr PxsMsgHeader
	bal uint32
}

//NewPxsMsgCommit :
func NewPxsMsgCommit(iid, bal uint32) *PxsMsgCommit {
	m := new(PxsMsgCommit)
	m.bal = bal
	m.hdr = PxsMsgHeader{
		Len: 4, Typ: PxsMsgTypeCommit, Iid: iid,
	}
	return m
}

//Encode :
func (m PxsMsgCommit) Encode() ([]byte, error) {
	var data = []interface{}{
		m.hdr, //header
		m.bal,
	}
	return serialize(data)
}

//DecodeOnePxsMsg : decode one msg, returns num of read bytes.
func DecodeOnePxsMsg(bs []byte) (msg interface{}, nrd uint32, err error) {
	rd := bytes.NewReader(bs)
	//1. header
	hdr := new(PxsMsgHeader)
	if err := binary.Read(rd, binary.LittleEndian, hdr); err != nil {
		goto WRONG_MSG_FORMAT
	}
	//2. parse all type of msg
	switch hdr.Typ {
	case PxsMsgTypeRequest:
		req := new(PxsMsgRequest)
		req.Hdr = *hdr
		//size
		if err = binary.Read(rd, binary.LittleEndian, &req.Val.Siz); err != nil {
			goto WRONG_MSG_FORMAT
		}
		if req.Val.Siz == 0 { // should not be 0 length
			goto WRONG_MSG_FORMAT
		}
		//octet
		req.Val.Oct = make([]byte, req.Val.Siz) //XXX should be fixed size array
		if err = binary.Read(rd, binary.LittleEndian, &req.Val.Oct); err != nil {
			goto WRONG_MSG_FORMAT
		}
		msg = req
	case PxsMsgTypePrepare:
		pre := new(PxsMsgPrepare)
		pre.Hdr = *hdr
		//bal
		if err = binary.Read(rd, binary.LittleEndian, &pre.Bal); err != nil {
			goto WRONG_MSG_FORMAT
		}
		msg = pre
	case PxsMsgTypePromise:
		pro := new(PxsMsgPromise)
		pro.hdr = *hdr
		//acc,bal,mVBal,mVal.Siz
		flds := []interface{}{
			&pro.acc, &pro.bal, &pro.mVBal,
			&pro.mVal.Siz,
		}
		if err = deserialize(flds, rd); err != nil {
			goto WRONG_MSG_FORMAT
		}
		log.Println("Promise - acc,bal,mvbal,mval.siz:",
			pro.acc, pro.bal, pro.mVBal, pro.mVal.Siz)
		//mVal.Oct
		if pro.mVal.Siz != 0 {
			pro.mVal.Oct = make([]byte, pro.mVal.Siz)
			if err = binary.Read(rd, binary.LittleEndian, &pro.mVal.Oct); err != nil {
				goto WRONG_MSG_FORMAT
			}
		}
		msg = pro
	case PxsMsgTypeAccept:
		acc := new(PxsMsgAccept)
		acc.hdr = *hdr
		//bal,val.siz
		flds := []interface{}{
			&acc.bal, &acc.val.Siz,
		}
		if err = deserialize(flds, rd); err != nil {
			goto WRONG_MSG_FORMAT
		}
		if acc.val.Siz != 0 {
			acc.val.Oct = make([]byte, acc.val.Siz)
			if err = binary.Read(rd, binary.LittleEndian, &acc.val.Oct); err != nil {
				goto WRONG_MSG_FORMAT
			}
		}
		msg = acc
	case PxsMsgTypeAccepted:
		acd := new(PxsMsgAccepted)
		acd.hdr = *hdr
		//acd.acc,acd.bal,acd.val.siz
		flds := []interface{}{
			&acd.acc, &acd.bal, &acd.val.Siz,
		}
		if err = deserialize(flds, rd); err != nil {
			goto WRONG_MSG_FORMAT
		}
		if acd.val.Siz != 0 {
			acd.val.Oct = make([]byte, acd.val.Siz)
			if err = binary.Read(rd, binary.LittleEndian, &acd.val.Oct); err != nil {
				goto WRONG_MSG_FORMAT
			}
		}
		msg = acd
	case PxsMsgTypeCommit:
		cmt := new(PxsMsgCommit)
		cmt.hdr = *hdr
		//bal
		if err = binary.Read(rd, binary.LittleEndian, &cmt.bal); err != nil {
			goto WRONG_MSG_FORMAT
		}
		msg = cmt
	case PxsMsgTypeResponse:
		rsp := new(PxsMsgResponse)
		rsp.Hdr = *hdr
		//ret
		if err = binary.Read(rd, binary.LittleEndian, &rsp.Ret); err != nil {
			goto WRONG_MSG_FORMAT
		}
		msg = rsp
	default:
		return nil, 0, errors.New("wrong PxsMsgType")
	}
	//num of read bytes.
	nrd = uint32(rd.Size()) - uint32(rd.Len())
	return msg, nrd, nil
WRONG_MSG_FORMAT:
	return nil, 0, err
}
