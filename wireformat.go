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
	siz uint32     //msg length
	typ PxsMsgType //msg type ID: 0a,1a,1b,2a,2b,3a,0b
	iid uint32     //instance ID or Sequence num of request.
}

//Value : Client Value, size < 64K
type Value struct {
	siz uint32 //0: Value is none.
	oct []byte //if oct == nil, then val == num;
}

//size : len of bytes
func (v Value) size() uint32 {
	return uint32(len(v.oct))
}

//IsNone : v is a None Value.
func (v Value) IsNone() bool {
	if v.siz == 0 || len(v.oct) == 0 {
		return true
	}
	return false
}

//////////////////////////////////////////////////////////////////////////////////

//PxsMsgRequest :
type PxsMsgRequest struct {
	hdr PxsMsgHeader // hdr.type = PxsMsgTypeRequest, hdr.iid as seq num of Value;
	val Value
}

//NewPxsMsgRequest :  new msg
func NewPxsMsgRequest(seq uint32, val *Value) *PxsMsgRequest {
	m := new(PxsMsgRequest)
	m.hdr = PxsMsgHeader{
		siz: 4 + val.size(), //val.siz + val.oct
		iid: seq,
		typ: PxsMsgTypeRequest,
	}
	m.val = *val
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
		m.hdr, m.val.siz, m.val.oct,
	}
	return serialize(data)
}

//PxsMsgResponse : P0b msg
type PxsMsgResponse struct {
	hdr PxsMsgHeader // hdr.type = PxsMsgResponse, hdr.iid as seq num of Value;
	ret uint32       // return code; OK or TIMEOUT;
}

//NewPxsMsgResponse :
func NewPxsMsgResponse(iid, ret uint32) *PxsMsgResponse {
	m := new(PxsMsgResponse)
	m.ret = ret
	m.hdr = PxsMsgHeader{
		siz: 4, typ: PxsMsgTypeResponse, iid: iid,
	}
	return m
}

//Encode : struct to bytes
func (m PxsMsgResponse) Encode() ([]byte, error) {
	var data = []interface{}{
		m.hdr, //header
		m.ret,
	}
	return serialize(data)
}

//PxsMsgPrepare :
type PxsMsgPrepare struct {
	hdr PxsMsgHeader
	bal uint32
}

//Invalidballot : None Value;
const Invalidballot = uint32(0xFFFFFFFF)

//NewPxsMsgPrepare :
func NewPxsMsgPrepare(iid, bal uint32) *PxsMsgPrepare {
	m := new(PxsMsgPrepare)
	m.hdr = PxsMsgHeader{
		siz: 4, iid: iid, typ: PxsMsgTypePrepare,
	}
	m.bal = bal
	return m
}

//Encode : struct to bytes
func (m PxsMsgPrepare) Encode() ([]byte, error) {
	var data = []interface{}{
		m.hdr, //header
		m.bal,
	}
	return serialize(data)
}

//PxsMsgPromise :
type PxsMsgPromise struct {
	hdr   PxsMsgHeader
	acc   uint32 //acceptor ID;
	bal   uint32 //bal > mbal;
	mVbal uint32 //voted ballot: if mbal != Invalidballot then mval is an old Value;
	mval  Value  //voted Value: old Value replied;
}

//NewPxsMsgPromise :
func NewPxsMsgPromise(iid, acc, bal, mVbal uint32, val *Value) *PxsMsgPromise {
	m := new(PxsMsgPromise)
	m.hdr = PxsMsgHeader{
		siz: 4*4 + val.siz, //acc,bal,mvbal, siz, oct
		typ: PxsMsgTypePromise,
		iid: iid,
	}
	m.acc = acc
	m.bal = bal
	m.mVbal = mVbal
	m.mval = *val
	return m
}

//Encode :
func (m PxsMsgPromise) Encode() ([]byte, error) {
	var data = []interface{}{
		m.hdr, //header
		m.acc, m.bal, m.mVbal,
		m.mval.siz, m.mval.oct,
	}
	return serialize(data)
}

//acceptors state:
//maxbal[a]  : promised ballot
//maxVbal[a] : accepted ballot
//maxval[a]  : accepted Value

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
		siz: 4 + 4 + m.val.siz, //bal,val.siz,val.oct
		typ: PxsMsgTypeAccept,
		iid: iid,
	}
	return m
}

//Encode :
func (m PxsMsgAccept) Encode() (bs []byte, err error) {
	var data = []interface{}{
		m.hdr, //header
		m.bal, m.val.siz, m.val.oct,
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
		siz: 4*3 + m.val.siz, //acc,bal,val.siz, val.oct
		typ: PxsMsgTypeAccepted,
		iid: iid,
	}
	return m
}

//Encode :
func (m PxsMsgAccepted) Encode() ([]byte, error) {
	var data = []interface{}{
		m.hdr, //header
		m.acc, m.bal, m.val.siz,
		m.val.oct,
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
		siz: 4, typ: PxsMsgTypeCommit, iid: iid,
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
	flds := []interface{}{
		&hdr.siz, &hdr.typ, &hdr.iid,
	}
	if err = deserialize(flds, rd); err != nil {
		goto WRONG_MSG_FORMAT
	}
	//2. parse all type of msg
	switch hdr.typ {
	case PxsMsgTypeRequest:
		req := new(PxsMsgRequest)
		req.hdr = *hdr
		//size
		if err = binary.Read(rd, binary.LittleEndian, &req.val.siz); err != nil {
			goto WRONG_MSG_FORMAT
		}
		if req.val.siz == 0 { // should not be 0 length
			goto WRONG_MSG_FORMAT
		}
		//octet
		req.val.oct = make([]byte, req.val.siz) //XXX should be fixed size array
		if err = binary.Read(rd, binary.LittleEndian, &req.val.oct); err != nil {
			goto WRONG_MSG_FORMAT
		}
		msg = req
	case PxsMsgTypePrepare:
		pre := new(PxsMsgPrepare)
		pre.hdr = *hdr
		//bal
		if err = binary.Read(rd, binary.LittleEndian, &pre.bal); err != nil {
			goto WRONG_MSG_FORMAT
		}
		msg = pre
	case PxsMsgTypePromise:
		pro := new(PxsMsgPromise)
		pro.hdr = *hdr
		//acc,bal,mVbal,mval.siz
		flds := []interface{}{
			&pro.acc, &pro.bal, &pro.mVbal,
			&pro.mval.siz,
		}
		if err = deserialize(flds, rd); err != nil {
			goto WRONG_MSG_FORMAT
		}
		log.Println("Promise - acc,bal,mvbal,mval.siz:",
			pro.acc, pro.bal, pro.mVbal, pro.mval.siz)
		//mval.oct
		if pro.mval.siz != 0 {
			pro.mval.oct = make([]byte, pro.mval.siz)
			if err = binary.Read(rd, binary.LittleEndian, &pro.mval.oct); err != nil {
				goto WRONG_MSG_FORMAT
			}
		}
		msg = pro
	case PxsMsgTypeAccept:
		acc := new(PxsMsgAccept)
		acc.hdr = *hdr
		//bal,val.siz
		flds := []interface{}{
			&acc.bal, &acc.val.siz,
		}
		if err = deserialize(flds, rd); err != nil {
			goto WRONG_MSG_FORMAT
		}
		if acc.val.siz != 0 {
			acc.val.oct = make([]byte, acc.val.siz)
			if err = binary.Read(rd, binary.LittleEndian, &acc.val.oct); err != nil {
				goto WRONG_MSG_FORMAT
			}
		}
		msg = acc
	case PxsMsgTypeAccepted:
		acd := new(PxsMsgAccepted)
		acd.hdr = *hdr
		//acd.acc,acd.bal,acd.val.siz
		flds := []interface{}{
			&acd.acc, &acd.bal, &acd.val.siz,
		}
		if err = deserialize(flds, rd); err != nil {
			goto WRONG_MSG_FORMAT
		}
		if acd.val.siz != 0 {
			acd.val.oct = make([]byte, acd.val.siz)
			if err = binary.Read(rd, binary.LittleEndian, &acd.val.oct); err != nil {
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
		rsp.hdr = *hdr
		//ret
		if err = binary.Read(rd, binary.LittleEndian, &rsp.ret); err != nil {
			goto WRONG_MSG_FORMAT
		}
		msg = rsp
	default:
		return nil, 0, errors.New("wrong PxsMsgType")
	}
	//num of read bytes: original_size - bytes_unread.
	nrd = uint32(rd.Size()) - uint32(rd.Len())
	return msg, nrd, nil
WRONG_MSG_FORMAT:
	return nil, 0, err
}
