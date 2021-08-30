package cmd

import "github.com/google/gopacket"

func captureContext(ci gopacket.CaptureInfo) *Context {
	return &Context{ci}
}

type Context struct {
	CaptureInfo gopacket.CaptureInfo
}

func (c *Context) GetCaptureInfo() gopacket.CaptureInfo {
	return c.CaptureInfo
}
