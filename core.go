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
	TagDB       = "db"
	TagOperator = "dbop"
	TagCol      = "dbcol"
	TagDive     = "dive"

	MysqlErrCodeDuplicateEntry                  = 1062
	MysqlErrCodeIncorrectValue                  = 1411
	MysqlErrCodeForeignKeyConstraintFailsCreate = 1452
	MysqlErrCodeForeignKeyConstraintFailsDelete = 1451

	MysqlIntMax  = 2147483647
	MysqlUintMax = 4294967295
)

//================================================================
// Prototype
//================================================================
type PrototypeInterface interface {
	Init()
}

type PrototypeTime struct {
	Ctime *time.Time `db:"ctime" json:"createdAt"`
	Mtime *time.Time `db:"mtime" json:"modifiedAt"`
}

func (pt *PrototypeTime) Init() {
	ctime := time.Now().UTC().Truncate(time.Second)
	mtime := ctime
	pt.Ctime = &ctime
	pt.Mtime = &mtime
}

type Prototype struct {
	ID *uuid.UUID `db:"id" json:"id"`
	PrototypeTime
}

func (p *Prototype) Init() {
	id := uuid.New()
	p.ID = &id
	p.PrototypeTime.Init()
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

type EngineInterface interface {
	NewRows() interface{}
	NewRow() interface{}
	Insert(assignments interface{}) (sql.Result, error)
	Has(conds interface{}) (bool, error)
	FetchRows(dest, conds interface{}, qp QueryParametersInterface, paginate bool) error
	FetchRow(dest, conds interface{}) error
	FetchByKey(dest, key interface{}) error
	Update(conds, assignments interface{}) (sql.Result, error)
	Delete(conds interface{}) (sql.Result, error)
}

func (e *Engine) NewRows() interface{} {
	return &[]*Prototype{}
}

func (e *Engine) NewRow() interface{} {
	return new(Prototype)
}

func (e *Engine) Insert(assignments interface{}) (sql.Result, error) {
	fields, placeholders := []string{}, []string{}
	genInsertAssignments(assignments, &fields, &placeholders)
	q := `INSERT INTO ` + e.TblName + ` (` + strings.Join(fields, ",") + `) VALUES (` + strings.Join(placeholders, ",") + `);`
	return e.NamedExec(q, assignments)
}

func (e *Engine) Has(conds interface{}) (bool, error) {
	flag, args := false, []interface{}{}
	conditions := strings.Join(*(genConditionsVar(conds, &args)), " AND ")
	q := `SELECT EXISTS(SELECT 1 FROM ` + e.TblName + ` WHERE ` + conditions + `);`
	err := e.Get(&flag, q, args...)
	return flag, err
}

func (e *Engine) FetchRows(dest, conds interface{}, qp QueryParametersInterface, paginate bool) error {
	args, conditions, hasPreCondition := []interface{}{}, "", false

	if conds != nil && !reflect.ValueOf(conds).IsNil() {
		conditions = ` ` + strings.Join(*(genConditionsVar(conds, &args)), " AND ")
		hasPreCondition = true
	}

	q := `SELECT * FROM ` + e.TblName + ` WHERE` + conditions + qp.Build(&args, hasPreCondition, paginate) + `;`
	return e.Select(&dest, q, args...)
}

func (e *Engine) FetchRow(dest, conds interface{}) error {
	args := []interface{}{}
	conditions := strings.Join(*(genConditionsVar(conds, &args)), " AND ")
	q := `SELECT * FROM ` + e.TblName + ` WHERE ` + conditions + `;`
	return e.Get(dest, q, conds)
}

// TODO: to be fixed
func (e *Engine) FetchByKey(dest, key interface{}) error {
	q := ""
	if _, ok := key.(uuid.UUID); ok {
		q = `SELECT * FROM ` + e.TblName + ` WHERE id = UUID_TO_BIN(?);`
	} else {
		q = `SELECT * FROM ` + e.TblName + ` WHERE identity = ?;`
	}

	return e.Get(dest, q, key)
}

//	TODO: bug happended when SET col = 'bb' WHERE col = 'aa';
func (e *Engine) Update(conds, assignments interface{}) (sql.Result, error) {
	args := map[string]interface{}{}
	assigns := strings.Join(*(genConditionsNamed(assignments, &args)), ", ")
	conditions := strings.Join(*(genConditionsNamed(conds, &args)), " AND ")
	q := `UPDATE ` + e.TblName + ` SET ` + assigns + ` WHERE ` + conditions + `;`
	return e.NamedExec(q, args)
}

func (e *Engine) Delete(conds interface{}) (sql.Result, error) {
	conditions := strings.Join(*(genConditionsNamed(conds, nil)), " AND ")
	q := `DELETE FROM ` + e.TblName + ` WHERE ` + conditions + `;`
	return e.NamedExec(q, conds)
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
// Misc
//----------------------------------------------------------------
func genInsertAssignments(assignments interface{}, fields, placeholders *[]string) {
	v := reflect.ValueOf(assignments)
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
			genInsertAssignments(val.Interface(), fields, placeholders)
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

func genConditionsVar(sour interface{}, args *[]interface{}) *[]string {
	assigns, v := []string{}, reflect.ValueOf(sour)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	length := v.NumField()
	for i := 0; i < length; i += 1 {
		val, struF := v.Field(i), v.Type().Field(i)
		dbCol, dbVal := struF.Tag.Get(TagCol), struF.Tag.Get(TagDB)
		if dbCol == "" {
			dbCol = dbVal
		}
		if isValidAssignment(val, dbCol) {
			operator := struF.Tag.Get(TagOperator)
			if operator == "" {
				operator = "="
			}
			if args != nil {
				*args = append(*args, val)
			}
			fmtStr := ""
			if strings.Contains(struF.Type.String(), "uuid.UUID") {
				fmtStr = "%s " + operator + " UUID_TO_BIN(?)"
			} else {
				fmtStr = "%s " + operator + " ?"
			}
			assigns = append(assigns, fmt.Sprintf(fmtStr, dbCol))
		}
	}

	return &assigns
}

func genConditionsNamed(sour interface{}, args *map[string]interface{}) *[]string {
	assigns, v := []string{}, reflect.ValueOf(sour)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	length := v.NumField()
	for i := 0; i < length; i += 1 {
		val, struF := v.Field(i), v.Type().Field(i)
		dbCol, dbVal := struF.Tag.Get(TagCol), struF.Tag.Get(TagDB)
		if dbCol == "" {
			dbCol = dbVal
		}
		if isValidAssignment(val, dbCol) {
			operator := struF.Tag.Get(TagOperator)
			if operator == "" {
				operator = "="
			}
			if args != nil {
				(*args)[dbCol] = val
			}
			fmtStr := ""
			if strings.Contains(struF.Type.String(), "uuid.UUID") {
				fmtStr = "%s " + operator + " UUID_TO_BIN(:%s)"
			} else {
				fmtStr = "%s " + operator + " :%s"
			}
			assigns = append(assigns, fmt.Sprintf(fmtStr, dbCol, dbVal))
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
