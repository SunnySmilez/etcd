package main

import (
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
