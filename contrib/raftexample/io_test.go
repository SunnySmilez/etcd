package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestMulti(t *testing.T) {
	reader1 := strings.NewReader("Geeks\n")
	reader2 := strings.NewReader("GfG\n")
	reader3 := strings.NewReader("CS\n")

	// Calling MultiReader method with its parameters
	r := io.MultiReader(reader1, reader2, reader3)

	// Calling Copy method with its parameters
	Reader, err := io.Copy(os.Stdout, r)

	// If error is not nil then panics
	if err != nil {
		panic(err)
	}

	// Prints output
	fmt.Printf("n:%v\n", Reader)
}

func TestIoReader(t *testing.T) {
	reader1 := strings.NewReader("this is test")
	b := make([]byte, 20)
	//b := []byte{}  // 这样是不行的
	n1, err := reader1.Read(b)
	if err != nil {
		fmt.Printf("err:%+v", err)
	}

	fmt.Printf("%+v\n", string(b[:n1]))

	reader := strings.NewReader("Clear is better than clever")
	p := make([]byte, 4)
	for {
		n, err := reader.Read(p)
		if err == io.EOF {
			break
		}
		fmt.Println(string(p[:n]))
	}
}

func TestWrite(t *testing.T) {
	proverbs := []string{
		"Channels orchestrate mutexes serialize",
		"Cgo is not Go",
		"Errors are values",
		"Don't panic",
	}
	var writer bytes.Buffer

	for _, p := range proverbs {
		n, err := writer.Write([]byte(p))
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		if n != len(p) {
			fmt.Println("failed to write data")
			os.Exit(1)
		}
	}

	fmt.Println(writer.String())
}
