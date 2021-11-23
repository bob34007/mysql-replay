/**
 * @Author: guobob
 * @Description:
 * @File:  stream_test.go
 * @Version: 1.0.0
 * @Date: 2021/11/22 17:14
 */

package stream

import (
	"bytes"
	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/reassembly"
	"github.com/stretchr/testify/assert"
	"reflect"
	"strings"
	"testing"
	"time"
)


func TestConnID_SrcAddr(t *testing.T) {

	tests := []struct {
		name string
		k    ConnID
		want string
	}{
		{
			name :"scraddr",
			k : ConnID{
			},
			want : "[]:[]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.k.SrcAddr(); got != tt.want {
				t.Errorf("SrcAddr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnID_DstAddr(t *testing.T) {
	tests := []struct {
		name string
		k    ConnID
		want string
	}{
		 {
			 name:"dstaddr ",
			 k:ConnID{},
			 want:"[]:[]",
		 },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.k.DstAddr(); got != tt.want {
				t.Errorf("DstAddr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnID_String(t *testing.T) {
	tests := []struct {
		name string
		k    ConnID
		want string
	}{
		{
			name:"string",
			k:ConnID{},
			want:"[]:[]->[]:[]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.k.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnID_Reverse(t *testing.T) {
	tests := []struct {
		name string
		k    ConnID
		want ConnID
	}{
		{
			name:"reverse",
			k:ConnID{},
			want :ConnID{},
		},
	}
		for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.k.Reverse(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Reverse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnID_Hash(t *testing.T) {
	tests := []struct {
		name string
		k    ConnID
		want uint64
	}{
		{
			name:"hash",
			k:ConnID{},
			want:1181368135640866778,
		},
	}
		for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.k.Hash(); got != tt.want {
				t.Errorf("Hash() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnID_HashStr(t *testing.T) {
	tests := []struct {
		name string
		k    ConnID
		want string
	}{
		{
			name:"hashstr",
			k:ConnID{},
			want:"dadfd6690f106510",
		},
	}
		for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.k.HashStr(); got != tt.want {
				t.Errorf("HashStr() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConnID_Logger_with_name_len_lg_zero(t *testing.T){
	k := ConnID{}
	logger :=k.Logger("test")
	assert.New(t).NotNil(logger)
}

func TestConnID_Logger_with_name_len_eq_zero(t *testing.T){
	k := ConnID{}
	logger :=k.Logger("")
	assert.New(t).NotNil(logger)
}

func Test_getPkt_with_pkt0 (t *testing.T){
	var dir reassembly.TCPFlowDirection = false
	s :=new(mysqlStream)
	s.pkt0=&MySQLPacket{
		Seq:1,
	}
	s.pkt1=&MySQLPacket{
		Seq:2,
	}
	pkt:=s.getPkt(dir)
	assert.New(t).Equal(1,pkt.Seq)
}

func Test_getPkt_with_pkt1 (t *testing.T){
	var dir reassembly.TCPFlowDirection = true
	s :=new(mysqlStream)
	s.pkt0=&MySQLPacket{
		Seq:1,
	}
	s.pkt1=&MySQLPacket{
		Seq:2,
	}
	pkt:=s.getPkt(dir)
	assert.New(t).Equal(2,pkt.Seq)
}

func Test_setPkt_with_pkt0 (t *testing.T){
	var dir reassembly.TCPFlowDirection = false
	s :=new(mysqlStream)
	pkt0:=&MySQLPacket{
		Seq:1,
	}

	s.setPkt(dir,pkt0)
	assert.New(t).Equal(1,s.pkt0.Seq)
}

func Test_setPkt_with_pkt1 (t *testing.T){
	var dir reassembly.TCPFlowDirection = true
	s :=new(mysqlStream)
	pkt1:=&MySQLPacket{
		Seq:2,
	}
	s.setPkt(dir,pkt1)
	assert.New(t).Equal(2,s.pkt1.Seq)
}

func Test_getTime_with_ts0(t *testing.T){
	var dir reassembly.TCPFlowDirection = false
	s :=new(mysqlStream)

	ts0:=time.Now()
	ts1:=time.Now().Add(5*time.Second)
	s.t0=ts0
	s.t1=ts1
	ts:=s.getTime(dir)

	assert.New(t).Equal(ts0,ts)

}

func Test_getTime_with_ts1(t *testing.T){
	var dir reassembly.TCPFlowDirection = true
	s :=new(mysqlStream)
	ts0:=time.Now()
	ts1:=time.Now().Add(5*time.Second)
	s.t0=ts0
	s.t1=ts1
	ts:=s.getTime(dir)

	assert.New(t).Equal(ts1,ts)

}

func Test_setTime_with_ts0(t *testing.T){
	var dir reassembly.TCPFlowDirection = false
	s :=new(mysqlStream)
	ts0:=time.Now()
	s.setTime(dir,ts0)
	assert.New(t).Equal(s.t0,ts0)
}
func Test_setTime_with_ts1(t *testing.T){
	var dir reassembly.TCPFlowDirection = true
	s :=new(mysqlStream)
	ts1:=time.Now().Add(5*time.Second)
	s.setTime(dir,ts1)
	assert.New(t).Equal(s.t1,ts1)
}

func Test_lookupPacketSeq_with_len(t *testing.T){
	buf :=new(bytes.Buffer)
	_,_=buf.Write([]byte("12345"))
	seq:=lookupPacketSeq(buf)
	assert.New(t).Equal(seq,52)
}

func Test_lookupPacketSeq_with_zero(t *testing.T){
	buf :=new(bytes.Buffer)
	_,_=buf.Write([]byte("123"))
	seq:=lookupPacketSeq(buf)
	assert.New(t).Equal(seq,-1)
}

func Test_formatData_len_lg_500(t *testing.T){
	a:=strings.Repeat("select",100)
	buf:=[]byte(a)
	want:= string(buf[:297]) + "..." + string(buf[len(buf)-200:])
	res:=formatData(buf)
	assert.New(t).Equal(want,res)
}
func Test_formatData_len_lt_500(t *testing.T){
	want:=strings.Repeat("select",2)
	buf:=[]byte(want)
	res:=formatData(buf)
	assert.New(t).Equal(want,res)
}

func Test_fnvHash(t *testing.T){
	params := make([][]byte,0)
	val1:=[]byte("12345")
	val2:=[]byte("abcde")
	val3:=[]byte("!@#$%")
	want :=uint64(11931777628171521584)
	params=append(params,val1,val2,val3)
	res:= fnvHash(params...)
	assert.New(t).Equal(res,want)
}

type ForTest1 struct{}
func (f * ForTest1)Accept(ci gopacket.CaptureInfo, dir reassembly.TCPFlowDirection, tcp *layers.TCP) bool{
	return true
}
func (f * ForTest1)OnPacket(pkt MySQLPacket){
	return
}
func (f * ForTest1)OnClose(){
	return
}


func Test_ReassemblyComplete_Asynchronized(t *testing.T){
	s:=new(mysqlStream)
	s.log=logger
	s.ch = make(chan MySQLPacket  ,100)
	s.opts.Synchronized = true
	s.h=&ForTest1{}
	var ac reassembly.AssemblerContext
	s.ReassemblyComplete(ac)
}



func Test_ReassemblyComplete_Synchronized(t *testing.T){
	s:=new(mysqlStream)
	s.log=logger
	s.ch = make(chan MySQLPacket  ,100)
	s.opts.Synchronized = false
	s.done =make(chan struct{},1)
	s.done<- ForTest{}
	s.h=&ForTest1{}
	var ac reassembly.AssemblerContext
	s.ReassemblyComplete(ac)
}


type ForTest2 struct{}
func (f * ForTest2)Accept(ci gopacket.CaptureInfo, dir reassembly.TCPFlowDirection, tcp *layers.TCP) bool{
	return false
}
func (f * ForTest2)OnPacket(pkt MySQLPacket){
	return
}
func (f * ForTest2)OnClose(){
	return
}

func Test_Accept_false (t *testing.T){
	tcp:=new(layers.TCP)
	var ci gopacket.CaptureInfo
	var dir reassembly.TCPFlowDirection
	var nextSeq reassembly.Sequence
	var start *bool
	var ac reassembly.AssemblerContext
	s:=new(mysqlStream)
	s.h=&ForTest2{}
	res:= s.Accept(tcp,ci,dir,nextSeq,start,ac)

	assert.New(t).False(res)
}

func Test_Accept_true (t *testing.T){
	tcp:=new(layers.TCP)
	var ci gopacket.CaptureInfo
	var dir reassembly.TCPFlowDirection
	var nextSeq reassembly.Sequence
	var start *bool
	var ac reassembly.AssemblerContext
	s:=new(mysqlStream)
	s.h=&ForTest1{}
	s.opts.Synchronized=true

	res:= s.Accept(tcp,ci,dir,nextSeq,start,ac)
	assert.New(t).True(res)
}

type ForTest3 struct {}
func (f *ForTest3) Accept(ci gopacket.CaptureInfo, dir reassembly.TCPFlowDirection, tcp *layers.TCP) bool{
	return false
}
func (f *ForTest3)OnPacket(pkt MySQLPacket){
	return
}
func (f *ForTest3)OnClose(){
	return
}


func Test_mysqlStreamFactory_New_Synchronized(t *testing.T){
	f:=new(mysqlStreamFactory)
	var netFlow, tcpFlow gopacket.Flow
	tcp :=new(layers.TCP)
	var ac reassembly.AssemblerContext

	f.new = func(conn ConnID) MySQLPacketHandler  {
		return &ForTest3{}
	}
	f.opts.Synchronized = true
	f.New(netFlow,tcpFlow,tcp,ac)
}

func Test_mysqlStreamFactory_New_Asynchronized(t *testing.T){
	f:=new(mysqlStreamFactory)
	var netFlow, tcpFlow gopacket.Flow
	tcp :=new(layers.TCP)
	var ac reassembly.AssemblerContext

	f.new = func(conn ConnID) MySQLPacketHandler  {
		return &ForTest3{}
	}
	f.opts.Synchronized = false
	f.New(netFlow,tcpFlow,tcp,ac)
}


func Test_getBuf_false (t *testing.T){
	s := new(mysqlStream)
	var dir reassembly.TCPFlowDirection =false
	buff0:=new(bytes.Buffer)
	buff0.Write([]byte("12345"))
	buff1:=new(bytes.Buffer)
	buff1.Write([]byte("abcde"))

	s.buf0=buff0
	s.buf1=buff1

	buff:=s.getBuf(dir)
	assert.New(t).Equal(buff,buff0)
}

func Test_getBuf_true (t *testing.T){
	s := new(mysqlStream)
	var dir reassembly.TCPFlowDirection =true
	buff0:=new(bytes.Buffer)
	buff0.Write([]byte("12345"))
	buff1:=new(bytes.Buffer)
	buff1.Write([]byte("abcde"))

	s.buf0=buff0
	s.buf1=buff1

	buff:=s.getBuf(dir)
	assert.New(t).Equal(buff,buff1)
}

func Test_setBuf_false (t *testing.T){
	s := new(mysqlStream)
	var dir reassembly.TCPFlowDirection =false
	buff0:=new(bytes.Buffer)
	buff0.Write([]byte("12345"))


	s.setBuf(dir,buff0)
	assert.New(t).Equal(s.buf0,buff0)
}

func Test_setBuf_true (t *testing.T){
	s := new(mysqlStream)
	var dir reassembly.TCPFlowDirection =true

	buff1:=new(bytes.Buffer)
	buff1.Write([]byte("abcde"))


	s.buf1=buff1

    s.setBuf(dir,buff1)
	assert.New(t).Equal(s.buf1,buff1)
}