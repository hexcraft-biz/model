package model

import (
	"database/sql"
	"fmt"
	"github.com/go-playground/validator/v10"
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

	MysqlErrCodeDuplicateEntry                  = 1062
	MysqlErrCodeIncorrectValue                  = 1411
	MysqlErrCodeForeignKeyConstraintFailsCreate = 1452
	MysqlErrCodeForeignKeyConstraintFailsDelete = 1451

	MysqlIntMax  = 2147483647
	MysqlUintMax = 4294967295
)

//================================================================
//
//================================================================
type PrototypeTime struct {
	Ctime time.Time `db:"ctime" json:"createdAt"`
	Mtime time.Time `db:"mtime" json:"modifiedAt"`
}

func (pt *PrototypeTime) InitTime() {
	pt.Ctime = time.Now().UTC().Truncate(time.Second)
	pt.Mtime = pt.Ctime
}

//================================================================
//
//================================================================
type Prototype struct {
	ID    *uuid.UUID `db:"id" json:"id"`
	Ctime *time.Time `db:"ctime" json:"createdAt"`
	Mtime *time.Time `db:"mtime" json:"modifiedAt"`
}

type PrototypeInterface interface {
	Init()
}

func (p *Prototype) Init() {
	id := uuid.New()
	ctime := time.Now().UTC().Truncate(time.Second)
	mtime := ctime

	p.ID, p.Ctime, p.Mtime = &id, &ctime, &mtime
}

func NewPrototype() *Prototype {
	id := uuid.New()
	ctime := time.Now().UTC().Truncate(time.Second)
	mtime := ctime
	return &Prototype{
		ID:    &id,
		Ctime: &ctime,
		Mtime: &mtime,
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

type EngineInterface interface {
	Insert(ams interface{}) (sql.Result, error)
	Has(ids interface{}) (bool, error)
	List(dest, ids interface{}, orderby, query string, searchCols []string, pg *Pagination) error
	GetByID(dest, id interface{}) error
	GetByKey(dest interface{}, key string) error
	GetByPrimaryKeys(dest, ids interface{}) error
	UpdateByPrimaryKeys(ids, assignments interface{}) (int64, error)
	DeleteByID(id interface{}) (int64, error)
	DeleteByPrimaryKeys(ids interface{}) (int64, error)
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
func (e *Engine) Insert(ams interface{}) (sql.Result, error) {
	fields, placeholders := []string{}, []string{}
	insertAssignments(ams, &fields, &placeholders)
	q := `INSERT INTO ` + e.TblName + ` (` + strings.Join(fields, ",") + `) VALUES (` + strings.Join(placeholders, ",") + `);`
	return e.NamedExec(q, ams)
}

func insertAssignments(ams interface{}, fields, placeholders *[]string) {
	v := reflect.ValueOf(ams)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	length := v.NumField()
	for i := 0; i < length; i++ {
		val, struF := v.Field(i), v.Type().Field(i)
		if val.Kind() == reflect.Ptr {
			if val.IsNil() {
				continue
			}
			val = val.Elem()
		}

		if _, ok := struF.Tag.Lookup(TagDive); ok {
			insertAssignments(val.Interface(), fields, placeholders)
		} else if dbCol := struF.Tag.Get(TagDB); dbCol != "" && dbCol != "-" {
			fmtStr := ""
			*fields = append(*fields, fmt.Sprintf("%s", dbCol))
			if strings.Contains(val.Type().String(), "uuid.UUID") {
				fmtStr = "UUID_TO_BIN(:%s)"
			} else {
				fmtStr = ":%s"
			}
			*placeholders = append(*placeholders, fmt.Sprintf(fmtStr, dbCol))
		}
	}
}

//----------------------------------------------------------------
// Select
//----------------------------------------------------------------
func (e *Engine) Has(ids interface{}) (bool, error) {
	flag, args := false, []interface{}{}
	conditions := strings.Join(*(genBindVarSql(ids, &args)), " AND ")
	q := `SELECT EXISTS(SELECT 1 FROM ` + e.TblName + ` WHERE ` + conditions + `);`
	err := e.Get(&flag, q, args...)
	return flag, err
}

func (e *Engine) List(dest, ids interface{}, qp QueryParametersInterface, paginate bool) error {
	args, conditions, hasPreCondition := []interface{}{}, "", false

	if ids != nil && !reflect.ValueOf(ids).IsNil() {
		conditions = ` ` + strings.Join(*(genBindVarSql(ids, &args)), " AND ")
		hasPreCondition = true
	}

	q := `SELECT * FROM ` + e.TblName + ` WHERE` + conditions + qp.Build(&args, hasPreCondition, paginate) + `;`
	return e.Select(&dest, q, args...)
}

func (e *Engine) GetByID(dest, id interface{}) error {
	q := `SELECT * FROM ` + e.TblName + ` WHERE id = UUID_TO_BIN(?);`
	return e.Get(dest, q, id)
}

func (e *Engine) GetByKey(dest interface{}, key string) error {
	q := ""
	if _, err := uuid.Parse(key); err == nil {
		q = `SELECT * FROM ` + e.TblName + ` WHERE id = UUID_TO_BIN(?);`
	} else {
		q = `SELECT * FROM ` + e.TblName + ` WHERE identity = ?;`
	}

	return e.Get(dest, q, key)
}

func (e *Engine) GetByPrimaryKeys(dest, ids interface{}) error {
	args := []interface{}{}
	conditions := strings.Join(*(genBindVarSql(ids, &args)), " AND ")
	q := `SELECT * FROM ` + e.TblName + ` WHERE ` + conditions + `;`
	return e.Get(dest, q, ids)
}

//----------------------------------------------------------------
// Update
//	TODO: UPDATE table SET col = 'bb' WHERE col = 'aa';
//----------------------------------------------------------------
func (e *Engine) UpdateByPrimaryKeys(ids, assignments interface{}) (int64, error) {
	args := map[string]interface{}{}
	assigns := strings.Join(*(genNamedSql(assignments, &args)), ", ")
	conditions := strings.Join(*(genNamedSql(ids, &args)), " AND ")
	q := `UPDATE ` + e.TblName + ` SET ` + assigns + ` WHERE ` + conditions + `;`
	if result, err := e.NamedExec(q, args); err != nil {
		return 0, err
	} else {
		return result.RowsAffected()
	}
}

//----------------------------------------------------------------
// Delete
//----------------------------------------------------------------
func (e *Engine) DeleteByID(id interface{}) (int64, error) {
	q := `DELETE FROM ` + e.TblName + ` WHERE id = UUID_TO_BIN(:id);`
	if result, err := e.NamedExec(q, map[string]interface{}{"id": id}); err != nil {
		return 0, err
	} else {
		return result.RowsAffected()
	}
}

func (e *Engine) DeleteByPrimaryKeys(ids interface{}) (int64, error) {
	conditions := strings.Join(*(genNamedSql(ids, nil)), " AND ")
	q := `DELETE FROM ` + e.TblName + ` WHERE ` + conditions + `;`
	if result, err := e.NamedExec(q, ids); err != nil {
		return 0, err
	} else {
		return result.RowsAffected()
	}
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

//----------------------------------------------------------------
// QueryParameters
//----------------------------------------------------------------
type QueryParametersInterface interface {
	Build(args *[]interface{}, hasPreCondition, paginate bool) string
	GenSearchCondition(args *[]interface{}, hasPreCondition bool) string
	GenOrderBy() string
	PaginationInterface
}

type QueryParameters struct {
	SearchQuery string   `form:"q" binding:"omitempty"`
	SearchCols  []string `form:"-" binding:"isdefault"`
	OrderBy     string   `form:"-" binding:"isdefault"`
	Pagination
}

func (qp *QueryParameters) Build(args *[]interface{}, hasPreCondition, paginate bool) string {
	q := qp.GenSearchCondition(args, hasPreCondition) + qp.GenOrderBy()
	if paginate {
		q += qp.Pagination.ToString(args)
	}
	return q
}

func (qp *QueryParameters) GenSearchCondition(args *[]interface{}, hasPreCondition bool) string {
	conditions := ""
	if qp.SearchQuery != "" && len(qp.SearchCols) > 0 {
		for i := range qp.SearchCols {
			qp.SearchCols[i] += " LIKE ?"
			*args = append(*args, "%"+qp.SearchQuery+"%")
		}

		if hasPreCondition {
			conditions = ` ` + strings.Join(qp.SearchCols, " OR ")
		} else {
			conditions = ` AND (` + strings.Join(qp.SearchCols, " OR ") + `)`
		}
	}

	return conditions
}

func (qp *QueryParameters) GenOrderBy() string {
	if qp.OrderBy != "" {
		return ` ORDER BY ` + qp.OrderBy
	} else {
		return ``
	}
}

//----------------------------------------------------------------
// Pagination
//----------------------------------------------------------------
const (
	PaginationDefaultOffset = 0
	PaginationDefaultLength = 16
	PaginationMinLength     = 1
	PaginationMaxLength     = 256
)

type PaginationInterface interface {
	ToString(args *[]interface{}) string
}

type Pagination struct {
	Offset uint64 `form:"pos" binding:"omitempty,validateOffset"`
	Length uint64 `form:"len" binding:"omitempty,validateLength"`
}

func (p *Pagination) ToString(args *[]interface{}) string {
	*args = append(*args, p.Offset, p.Length)
	return ` LIMIT ?, ?`
}

func ValidatorPaginationOffset(fl validator.FieldLevel) bool {
	v := fl.Field()
	if _, ok := v.Interface().(uint64); !ok {
		v.Set(reflect.ValueOf(0))
	}
	return true
}

func ValidatorPaginationLength(fl validator.FieldLevel) bool {
	v := fl.Field()
	if length, ok := v.Interface().(uint64); !ok {
		v.Set(reflect.ValueOf(PaginationMinLength))
	} else {
		switch {
		case length == 0:
			v.Set(reflect.ValueOf(PaginationDefaultLength))
		case length > 0 && length < PaginationMinLength:
			v.Set(reflect.ValueOf(PaginationMinLength))
		case length > PaginationMaxLength:
			v.Set(reflect.ValueOf(PaginationMaxLength))
		}
	}
	return true
}

//----------------------------------------------------------------
// Bindvar
//----------------------------------------------------------------
func genBindVarSql(sour interface{}, args *[]interface{}) *[]string {
	assigns, v := []string{}, reflect.ValueOf(sour)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	length := v.NumField()
	for i := 0; i < length; i += 1 {
		val, struF := v.Field(i), v.Type().Field(i)
		dbCol := struF.Tag.Get(TagDB)
		if isValidAssignment(val, dbCol) {
			if args != nil {
				*args = append(*args, val)
			}
			fmtStr := ""
			if strings.Contains(struF.Type.String(), "uuid.UUID") {
				fmtStr = "%s = UUID_TO_BIN(?)"
			} else {
				fmtStr = "%s = ?"
			}
			assigns = append(assigns, fmt.Sprintf(fmtStr, dbCol))
		}
	}

	return &assigns
}

//----------------------------------------------------------------
// Named
//----------------------------------------------------------------
func genNamedSql(sour interface{}, args *map[string]interface{}) *[]string {
	assigns, v := []string{}, reflect.ValueOf(sour)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	length := v.NumField()
	for i := 0; i < length; i += 1 {
		val, struF := v.Field(i), v.Type().Field(i)
		dbCol := struF.Tag.Get(TagDB)
		if isValidAssignment(val, dbCol) {
			if args != nil {
				(*args)[dbCol] = val
			}
			fmtStr := ""
			if strings.Contains(struF.Type.String(), "uuid.UUID") {
				fmtStr = "%s = UUID_TO_BIN(:%s)"
			} else {
				fmtStr = "%s = :%s"
			}
			assigns = append(assigns, fmt.Sprintf(fmtStr, dbCol, dbCol))
		}
	}

	return &assigns
}

func isValidAssignment(v reflect.Value, dbCol string) bool {
	if dbCol == "" || dbCol == "-" {
		return false
	} else if v.Kind() != reflect.Ptr {
		return true
	} else if v.Kind() == reflect.Ptr && !v.IsNil() {
		return true
	}
	return false
}

//----------------------------------------------------------------
// Misc
//----------------------------------------------------------------
func IsSlice(t interface{}) bool {
	switch reflect.TypeOf(t).Kind() {
	case reflect.Slice:
		return true
	default:
		return false
	}
}
