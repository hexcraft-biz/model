package model

import (
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"reflect"
	"strings"
	"time"
)

const (
	TagDB     = "db"
	TagAttach = "attach"
	TagDive   = "dive"

	DefaultOffset = 0
	DefaultLength = 20
	MinLength     = 1
	MaxLength     = 100
)

//================================================================
//
//================================================================
type Prototype struct {
	ID    *uuid.UUID `db:"id" json:"id"`
	Ctime *time.Time `db:"ctime" json:"createdAt"`
	Mtime *time.Time `db:"mtime" json:"modifiedAt"`
}

func (p *Prototype) Init() {
	ts, id := time.Now().UTC().Truncate(time.Second), uuid.New()
	p.ID = &id
	p.Ctime = &ts
	p.Mtime = &ts
}

func NewPrototype() *Prototype {
	ts, id := time.Now().UTC().Truncate(time.Second), uuid.New()
	return &Prototype{
		ID:    &id,
		Ctime: &ts,
		Mtime: &ts,
	}
}

//================================================================
//
//================================================================
func attach(dest, sour interface{}) {
	dv := reflect.ValueOf(dest)
	for dv.Kind() == reflect.Ptr {
		dv = dv.Elem()
	}

	for i := 0; i < dv.NumField(); i += 1 {
		fVal, fStru := dv.Field(i), dv.Type().Field(i)
		if names, ok := fStru.Tag.Lookup(TagDive); ok {
			if names != "" && names != "-" {
				if _, ok := findField(sour, strings.Split(names, ".")); ok {
					if fVal.IsNil() {
						typ := fStru.Type
						for typ.Kind() == reflect.Ptr {
							typ = typ.Elem()
						}
						fVal.Set(reflect.New(typ))
					}

					attach(fVal.Interface(), sour)
				}
			}
		} else {
			if names, ok := fStru.Tag.Lookup(TagAttach); ok {
				if sfv, ok := findField(sour, strings.Split(names, ".")); ok {
					assignValue(fVal, sfv)
				}
			}
		}
	}
}

func assignValue(dv, sv reflect.Value) {
	// TODO:
	sourType, destType := sv.Type().String(), dv.Type().String()
	// fmt.Println("sourType:", sourType, "CanAddr:", sv.CanAddr(), "destType:", destType)
	switch sourType {
	case "time.Time":
		switch destType {
		case "string":
			dv.Set(reflect.ValueOf(sv.Interface().(time.Time).Format(time.RFC3339)))
		case "*string":
			t := sv.Interface().(time.Time).Format(time.RFC3339)
			dv.Set(reflect.ValueOf(&t))
		case "*time.Time":
			dv.Set(sv.Addr())
		}
	//case "uuid.UUID":
	//	switch destType {
	//	case "string":
	//		dv.Set(reflect.ValueOf(sv.Interface().(uuid.UUID).String()))
	//	}
	case "string":
		switch destType {
		case "string":
			dv.Set(reflect.ValueOf(sv.Interface().(string)))
		case "*string":
			t := sv.Interface().(string)
			dv.Set(reflect.ValueOf(&t))
		}
	default:
		if strings.HasPrefix(destType, "*") {
			dv.Set(sv.Addr())
		} else {
			dv.Set(sv)
		}
	}
	// TODO:
}

func findField(sour interface{}, fieldNames []string) (reflect.Value, bool) {
	v := reflect.ValueOf(sour)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	var rstV reflect.Value
	for i := range fieldNames {
		rstV = v.FieldByName(fieldNames[i])
		if !rstV.IsValid() || (rstV.Kind() == reflect.Ptr && rstV.IsZero()) {
			return reflect.Value{}, false
		}
		for rstV.Kind() == reflect.Ptr {
			rstV = rstV.Elem()
		}
		v = rstV
	}

	return rstV, true
}

//================================================================
//
//================================================================
type Engine struct {
	*sqlx.DB
	TblName string
}

func NewEngine(db *sqlx.DB, tblName string) *Engine {
	return &Engine{
		DB:      db,
		TblName: tblName,
	}
}

//----------------------------------------------------------------
// Insert
//----------------------------------------------------------------
func (e *Engine) Insert(ams interface{}) (*ResultSet, error) {
	fields, placeholders := []string{}, []string{}
	insertAssignments(ams, &fields, &placeholders)
	q := `INSERT INTO ` + e.TblName + ` (` + strings.Join(fields, ",") + `) VALUES (` + strings.Join(placeholders, ",") + `);`
	_, err := e.NamedExec(q, ams)
	return NewResultSet(ams), err
}

func insertAssignments(ams interface{}, fields, placeholders *[]string) {
	v := reflect.ValueOf(ams)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for i := 0; i < v.NumField(); i++ {
		val, struF := v.Field(i), v.Type().Field(i)
		if val.Kind() == reflect.Ptr {
			if val.IsNil() {
				continue
			}
			val = val.Elem()
		}

		if _, ok := struF.Tag.Lookup(TagDive); ok {
			insertAssignments(val.Interface(), fields, placeholders)
		} else if ctag := struF.Tag.Get("db"); ctag != "" && ctag != "-" {
			fmtStr := ""
			*fields = append(*fields, fmt.Sprintf("%s", ctag))
			if strings.Contains(val.Type().String(), "uuid.UUID") {
				fmtStr = "UUID_TO_BIN(:%s)"
			} else {
				fmtStr = ":%s"
			}
			*placeholders = append(*placeholders, fmt.Sprintf(fmtStr, ctag))
		}
	}
}

//----------------------------------------------------------------
// Select
//----------------------------------------------------------------
func (e *Engine) Has(uuidStr string) (bool, error) {
	if _, err := uuid.Parse(uuidStr); err != nil {
		return false, nil
	}

	exists := false
	err := e.Get(&exists, `SELECT EXISTS(SELECT 1 FROM `+e.TblName+` WHERE id = UUID_TO_BIN(?));`, uuidStr)
	return exists, err
}

func (e *Engine) GetByID(id string, dst interface{}) (*ResultSet, error) {
	if u, err := uuid.Parse(id); err != nil {
		return nil, nil
	} else {
		q := `SELECT * FROM ` + e.TblName + ` WHERE id = UUID_TO_BIN(?);`
		if err := e.Get(dst, q, u); err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			} else {
				return nil, err
			}
		}

		return NewResultSet(dst), nil
	}
}

//----------------------------------------------------------------
// Update
//----------------------------------------------------------------
func (e *Engine) UpdateByID(id string, assignments interface{}) (int64, error) {
	if u, err := uuid.Parse(id); err != nil {
		return 0, nil
	} else {
		assigns, args := UpdateAssignments(assignments)
		args = append(args, u)
		q := `UPDATE ` + e.TblName + ` SET ` + assigns + ` WHERE id = UUID_TO_BIN(?);`
		if rst, err := e.Exec(q, args...); err != nil {
			return 0, err
		} else {
			return rst.RowsAffected()
		}
	}
}

func UpdateAssignments(ams interface{}) (string, []interface{}) {
	assigns, args, v := []string{}, []interface{}{}, reflect.ValueOf(ams)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for i := 0; i < v.NumField(); i += 1 {
		val, struF := v.Field(i), v.Type().Field(i)
		ctag := struF.Tag.Get(TagDB)
		if isUpdatedAssignment(val, ctag) {
			fmtStr := ""
			if strings.Contains(struF.Type.String(), "uuid.UUID") {
				fmtStr = "%s = UUID_TO_BIN(?)"
			} else {
				fmtStr = "%s = ?"
			}
			assigns = append(assigns, fmt.Sprintf(fmtStr, ctag))
			args = append(args, val.Interface())
		}
	}

	return strings.Join(assigns, ","), args
}

func isUpdatedAssignment(v reflect.Value, ctag string) bool {
	if ctag == "" || ctag == "-" {
		return false
	} else if v.Kind() != reflect.Ptr {
		return true
	} else if v.Kind() == reflect.Ptr && !v.IsNil() {
		return true
	}
	return false
}

//----------------------------------------------------------------
// Delete
//----------------------------------------------------------------
func (e *Engine) DeleteByID(id string) (int64, error) {
	if u, err := uuid.Parse(id); err != nil {
		return 0, nil
	} else {
		q := `DELETE FROM ` + e.TblName + ` WHERE id = UUID_TO_BIN(?);`
		if rst, err := e.Exec(q, u); err != nil {
			return 0, err
		} else {
			return rst.RowsAffected()
		}
	}
}

//----------------------------------------------------------------
// Pagination
//----------------------------------------------------------------
type Pagination struct {
	Offset uint64
	Length uint64
}

func NewDefaultPagination() *Pagination {
	return NewPagination(DefaultOffset, DefaultLength)
}

func NewPagination(offset, length uint64) *Pagination {
	return &Pagination{
		Offset: offset,
		Length: validLength(length),
	}
}

func (p *Pagination) Set(offset, length uint64) *Pagination {
	p.Offset = offset
	p.Length = validLength(length)
	return p
}

func (p *Pagination) ToString() string {
	return fmt.Sprintf(` LIMIT %d, %d`, p.Offset, p.Length)
}

func validLength(length uint64) uint64 {
	switch {
	case length == 0:
		length = DefaultLength
	case length > 0 && length < MinLength:
		length = MinLength
	case length > MaxLength:
		length = MaxLength
	}

	return length
}

//================================================================
// ResultSet
//================================================================
type ResultSet struct {
	Result       interface{}
	abstractType reflect.Type
}

func NewResultSet(r interface{}) *ResultSet {
	return &ResultSet{Result: r, abstractType: nil}
}

func (rs *ResultSet) AppliedBy(abs interface{}) *ResultSet {
	v := reflect.ValueOf(abs)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	rs.abstractType = v.Type()
	return rs
}

func (rs *ResultSet) GetRow() interface{} {
	var row interface{}

	if reflect.TypeOf(rs.Result).Kind() == reflect.Slice {
		row = reflect.ValueOf(rs.Result).Index(0).Interface()
	} else {
		row = rs.Result
	}

	absRow := reflect.New(rs.abstractType).Interface()
	attach(absRow, row)
	return absRow
}

func (rs *ResultSet) GetRows() []interface{} {
	var absRows []interface{}

	if reflect.TypeOf(rs.Result).Kind() == reflect.Slice {
		v := reflect.ValueOf(rs.Result)
		length := v.Len()
		absRows = make([]interface{}, length)
		for i := 0; i < length; i += 1 {
			absRows[i] = reflect.New(rs.abstractType).Interface()
			attach(absRows[i], v.Index(i).Interface())
		}
	} else {
		absRows = make([]interface{}, 1)
		absRows[0] = reflect.New(rs.abstractType).Interface()
		attach(absRows[0], rs.Result)
	}

	return absRows
}
