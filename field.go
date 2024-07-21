package gopgdbgen

import (
	"fmt"
	"strings"
)

type Field struct {
	Name         string `json:"name"`
	Description  string `json:"descr"`
	DataType     string `json:"type"`
	Length       int    `json:"length"`
	Precision    int    `json:"precision"`
	Nullable     bool   `json:"nullable"`
	DefaultValue string `json:"default"`
	IsPrimaryKey bool
}

var FieldProp []string = []string{"name", "descr", "type", "length", "precision", "nullable", "default"}

func (f *Field) CreateFieldDdl() string {
	var datatype string
	if f.Precision > 0 {
		datatype = fmt.Sprintf("%s(%d, %d)", f.DataType, f.Length, f.Precision)
	} else if f.Length > 0 {
		datatype = fmt.Sprintf("%s(%d)", f.DataType, f.Length)
	} else {
		datatype = f.DataType
	}
	fddl := []string{f.Name, datatype}

	if !f.Nullable {
		fddl = append(fddl, "not null")
	}

	if f.DefaultValue != "" {
		fddl = append(fddl, fmt.Sprintf("default %s", f.DefaultValue))
	}

	return strings.Join(fddl, " ")
}

func (f *Field) AddFieldDdl() string {
	return fmt.Sprintf("\tadd column if not exists %s", f.CreateFieldDdl())
}

func (f *Field) AlterFieldDdl() []string {
	var datatype string
	if f.Precision > 0 {
		datatype = fmt.Sprintf("%s(%d, %d)", f.DataType, f.Length, f.Precision)
	} else if f.Length > 0 {
		datatype = fmt.Sprintf("%s(%d)", f.DataType, f.Length)
	} else {
		datatype = f.DataType
	}

	altercolumnstpl := fmt.Sprintf("\talter column %s", f.Name)
	fddls := []string{fmt.Sprintf("%s type %s", altercolumnstpl, datatype)}

	if !f.Nullable {
		fddls = append(fddls, fmt.Sprintf("%s set not null", altercolumnstpl))
	} else {
		fddls = append(fddls, fmt.Sprintf("%s drop not null", altercolumnstpl))
	}

	if f.DefaultValue != "" {
		if f.DataType == "varchar" {
			fddls = append(fddls, fmt.Sprintf("%s set default '%s'", altercolumnstpl, f.DefaultValue))
		} else {
			fddls = append(fddls, fmt.Sprintf("%s set default %s", altercolumnstpl, f.DefaultValue))
		}
	} else {
		fddls = append(fddls, fmt.Sprintf("%s drop default", altercolumnstpl))
	}

	return fddls
}
