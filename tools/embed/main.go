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

	allValuesString := ""
	allValuesString = "\n// AllValues contains a list of all strings defined above\n"
	allValuesString += fmt.Sprintf("var AllValues = []string{")
	if len(jsonFiles) > 0 {
		for _, j := range jsonFiles {
			f, err := os.Open(j)
			if err != nil {
				panic(err)
			}
			variableName := strings.TrimSuffix(f.Name(), suffix) + goSuffix
			variableName = toUpperCamelCase(variableName)
			allValuesString += variableName + ", "

			out.Write([]byte("    // " + variableName + " ...\n"))
			out.Write([]byte("    " + variableName + " = `"))
			io.Copy(out, f)
			out.Write([]byte("`\n"))
		}
	}
	allValuesString = strings.TrimSuffix(allValuesString, ", ") + "}\n"
	out.Write([]byte(")\n"))
	out.Write([]byte(allValuesString))
}

// toUpperCamelCase convert a string to upper camel case
func toUpperCamelCase(str string) string {
	isToUpper := false
	var camelCase string
	for _, v := range str {
		if isToUpper {
			camelCase += strings.ToUpper(string(v))
			isToUpper = false
		} else {
			if v == '_' {
				isToUpper = true
			} else {
				camelCase += string(v)
			}
		}

	}
	return strings.Title(camelCase)

}
