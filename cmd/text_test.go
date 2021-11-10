package cmd

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCmd_GenerateFileSeqString(t *testing.T) {
	var seq =10
	wantStr :="-10"
	str:=GenerateFileSeqString(seq)

	ast := assert.New(t)
	ast.Equal(str,wantStr)
}

