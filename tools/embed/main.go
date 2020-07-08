package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

var fileType = flag.String("type", "json", "the type of files")
var packageName = flag.String("package", "main", "the package name")

func main() {
	flag.Parse()
	suffix := "." + *fileType
	goSuffix := strings.ToUpper(*fileType)
	fs, _ := ioutil.ReadDir(".")
	out, err := os.Create("generated_embedded_" + *fileType + ".go")
	if err != nil {
		panic(err)
	}
	out.Write([]byte(fmt.Sprintf("package %s \n", *packageName)))
	out.Write([]byte("\nconst (\n"))
	var jsonFiles []string
	for _, f := range fs {
		if strings.HasSuffix(f.Name(), suffix) {
			jsonFiles = append(jsonFiles, f.Name())
		}
	}

	if len(jsonFiles) > 0 {
		for _, j := range jsonFiles {
			f, err := os.Open(j)
			if err != nil {
				panic(err)
			}
			out.Write([]byte(strings.TrimSuffix(f.Name(), suffix) + goSuffix + " = `"))
			io.Copy(out, f)
			out.Write([]byte("`\n"))
		}
	}
	out.Write([]byte(")\n"))
}
