package gopgdbgen

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"slices"
	"strings"

	"github.com/valyala/fastjson"
)

type Table struct {
	Schema      string               `json:"schema"`
	TableName   string               `json:"table"`
	Description string               `json:"descr"`
	Fields      map[string]*Field    `json:"fields"`
	PrimaryKeys []string             `json:"primarykeys"`
	Uniques     map[string]*[]string `json:"uniques"`
	Relations   map[string]Relation  `json:"relations"`

	FieldNames []string
	PropNames  []string
}

var TableProp []string = []string{"schema", "table", "descr", "fields", "primarykeys", "uniques", "relations"}

func NewTable(jsondata []byte) (tbl *Table, err error) {
	json.Unmarshal(jsondata, &tbl)

	// tambah data default pada table
	madatoryfields := tbl.AddMandatoryFields()

	// baca urutan fields
	var v *fastjson.Value
	var p fastjson.Parser
	v, err = p.ParseBytes(jsondata)
	if err != nil {
		return tbl, err
	}

	var obj *fastjson.Object
	obj, err = v.Object()
	if err != nil {
		return tbl, err
	}
	obj.Visit(func(k []byte, v *fastjson.Value) {
		key := string(k)
		tbl.PropNames = append(tbl.PropNames, key)
	})

	// cek apakah propnames aman
	for _, k := range tbl.PropNames {
		if !slices.Contains(TableProp, k) {
			return tbl, fmt.Errorf("unknown property %s in table %s", k, tbl.TableName)
		}
	}

	errors := []string{}
	fields := v.GetObject("fields")
	fields.Visit(func(k []byte, v *fastjson.Value) {
		key := string(k)
		tbl.FieldNames = append(tbl.FieldNames, key)

		props, err := v.Object()
		if err != nil {
			eKey := fmt.Sprintf("%s%s%s", ColorRed, key, ColorReset)
			errors = append(errors, fmt.Errorf("cannot obtain object from json value '%s' : %s", eKey, err).Error())
		}

		props.Visit(func(k []byte, v *fastjson.Value) {
			propname := string(k)
			if !slices.Contains(FieldProp, propname) {
				ePropName := fmt.Sprintf("%s%s%s", ColorRed, propname, ColorReset)
				eKey := fmt.Sprintf("%s%s%s", ColorRed, key, ColorReset)
				errors = append(errors, fmt.Errorf("property %s pada field %s tidak dikenal", ePropName, eKey).Error())
			}
		})

	})
	tbl.FieldNames = append(tbl.FieldNames, madatoryfields...)

	if len(errors) > 0 {
		for _, e := range errors {
			fmt.Println(e)
		}
		eTableName := fmt.Sprintf("%s%s%s", ColorRed, tbl.TableName, ColorReset)
		return tbl, fmt.Errorf("ada kesalahan dalam definisi properti fields pada table %s", eTableName)
	}
	return tbl, err
}

func (tbl *Table) WriteSql(sqlfilepath string) error {
	fmt.Println("writing sql file:", path.Base(sqlfilepath))
	file, err := os.Create(sqlfilepath)
	if err != nil {
		return err
	}

	fmt.Fprintln(file, "-- DDL Generator : Table --")
	tbl.sqlCreateTable(file)
	tbl.sqlAddTableField(file)
	tbl.sqlAlterTableField(file)
	tbl.sqlSetTableConstratint(file)

	return nil
}

func (tbl *Table) sqlCreateTable(file *os.File) {
	fmt.Fprintf(file, "create table if not exists %s.%s (\n", tbl.Schema, tbl.TableName)
	for _, name := range tbl.PrimaryKeys {
		tbl.Fields[name].IsPrimaryKey = true
		tbl.Fields[name].Name = name
		f := tbl.Fields[name]

		fddl := "\t" + f.CreateFieldDdl() + ","
		fmt.Fprintln(file, fddl)
	}
	fmt.Fprintf(file, "\tprimary key (%s)\n", strings.Join(tbl.PrimaryKeys[:], ", "))
	fmt.Fprintln(file, ");")
	fmt.Fprintln(file, "")

	comment := fmt.Sprintf("comment on table %s.%s is '%s';", tbl.Schema, tbl.TableName, tbl.Description)
	fmt.Fprintln(file, comment)
	fmt.Fprintln(file, "")

}

func (tbl *Table) sqlAddTableField(file *os.File) {
	colums := []string{}

	for _, name := range tbl.FieldNames {
		tbl.Fields[name].Name = name
		f := tbl.Fields[name]
		if f.IsPrimaryKey {
			continue
		}
		colums = append(colums, f.AddFieldDdl())
	}

	fmt.Fprintln(file, "-- add new column")
	fmt.Fprintf(file, "alter table %s.%s\n", tbl.Schema, tbl.TableName)
	fmt.Fprintln(file, strings.Join(colums[:], ",\n"))
	fmt.Fprintln(file, ";")
	fmt.Fprintln(file, "")
}

func (tbl *Table) sqlAlterTableField(file *os.File) {
	colums := []string{}

	for _, name := range tbl.FieldNames {
		f := tbl.Fields[name]
		if f.IsPrimaryKey {
			continue
		}
		fddls := f.AlterFieldDdl()
		colums = append(colums, fddls...)
	}

	fmt.Fprintln(file, "-- alter column")
	fmt.Fprintf(file, "alter table %s.%s\n", tbl.Schema, tbl.TableName)
	fmt.Fprintln(file, strings.Join(colums[:], ",\n"))
	fmt.Fprintln(file, ";")
	fmt.Fprintln(file, "")
}

func (tbl *Table) sqlSetTableConstratint(file *os.File) {
	for name, fields := range tbl.Uniques {
		fmt.Fprintf(file, "alter table %s.%s drop constraint if exists %s;\n", tbl.Schema, tbl.TableName, name)
		fmt.Fprintf(file, "alter table %s.%s add constraint %s unique (%s);\n", tbl.Schema, tbl.TableName, name, strings.Join(*fields, ", "))
	}
}

func (tbl *Table) GetRelations() []*Relation {
	var relations []*Relation

	for key, rel := range tbl.Relations {
		rel.Key = key
		rel.Table = tbl.Schema + "." + tbl.TableName
		relations = append(relations, &rel)
	}
	return relations
}

func (tbl *Table) AddMandatoryFields() []string {
	tbl.Fields["createby"] = &Field{
		Description: "dibuat oleh",
		DataType:    "varchar",
		Length:      64,
	}

	tbl.Fields["createdate"] = &Field{
		Description:  "waktu dibuat",
		DataType:     "timestamp",
		Nullable:     false,
		DefaultValue: "now()",
	}

	tbl.Fields["modifyby"] = &Field{
		Description: "terkahir dimodifikasi oleh",
		DataType:    "varchar",
		Length:      64,
	}

	tbl.Fields["modifydate"] = &Field{
		Description: "waktu terakhir dimodifikasi",
		DataType:    "timestamp",
	}

	madatoryfields := []string{"createby", "createdate", "modifyby", "modifydate"}
	return madatoryfields
}
