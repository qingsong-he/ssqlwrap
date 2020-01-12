package ssqlwrap

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type cacheTypeInfo struct {
	elemByType reflect.Type
	fieldNames map[string]interface{}
	ch         chan struct{}
}

var (
	split                   = `\/`
	cacheMap                = &sync.Map{}
	isNotPtrOfStructTypeErr = errors.New(`is not '*[]struct' type`)
	noFeildStructErr        = errors.New("must be one feild at least")
	fieldNotBeStructErr     = errors.New("field can not be struct")
	fieldNotBeAnonymous     = errors.New("field can not be anonymous")
	noSQLTagErr             = errors.New("tag 'sql' is empty")
	colDupErr               = errors.New("not allow dup col")
)

func Query(db *sql.DB, objectModel interface{}, query string, args ...interface{}) error {

	// assume objectModel type is *[]struct
	tp := reflect.TypeOf(objectModel)
	kd := tp.Kind()
	switch kd {
	case reflect.Ptr:
		if tp.Elem().Kind() != reflect.Slice {
			return isNotPtrOfStructTypeErr
		}
		if tp.Elem().Elem().Kind() != reflect.Struct {
			return isNotPtrOfStructTypeErr
		}
	default:
		return isNotPtrOfStructTypeErr
	}

	// get value of slice
	result := reflect.ValueOf(objectModel).Elem()

	// get type of struct
	elemByType := reflect.TypeOf(objectModel).Elem().Elem()

	// check field count
	if elemByType.NumField() == 0 {
		return noFeildStructErr
	}

	// cache type info
	typeMark := elemByType.PkgPath() + "." + elemByType.Name()
	cacheTypeInfoInst, has := cacheMap.LoadOrStore(typeMark, &cacheTypeInfo{elemByType: elemByType, ch: make(chan struct{})})
	real := cacheTypeInfoInst.(*cacheTypeInfo)
	if !has {
		fieldNames := make(map[string]interface{})
		for i := 0; i < elemByType.NumField(); i++ {
			if elemByType.Field(i).Type.Kind() == reflect.Struct {
				close(real.ch)
				cacheMap.Delete(typeMark)
				return fieldNotBeStructErr
			}

			if elemByType.Field(i).Anonymous {
				close(real.ch)
				cacheMap.Delete(typeMark)
				return fieldNotBeAnonymous
			}

			tag := elemByType.Field(i).Tag.Get("sql")
			if tag == "" {
				close(real.ch)
				cacheMap.Delete(typeMark)
				return noSQLTagErr
			}

			fieldNames[tag+split+fmt.Sprintf("%d", i)] = nil
		}
		real.fieldNames = fieldNames
		close(real.ch)
	} else {
		<-real.ch
	}

	// alloc type with cache type info
	elemByValue := reflect.New(elemByType)
	fieldNames := make(map[string]interface{})
	for k, _ := range real.fieldNames {
		fieldKey := strings.Split(k, split)
		index, _ := strconv.Atoi(fieldKey[1])
		fieldNames[fieldKey[0]] = elemByValue.Elem().Field(index).Addr().Interface()
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	// check dup column
	types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	rowTypes := make([]interface{}, 0)
	dupColCheck := map[string]struct{}{}
	for _, v := range types {
		if _, ok := dupColCheck[v.Name()]; ok {
			return colDupErr
		}
		dupColCheck[v.Name()] = struct{}{}
	}

	// map tag 'sql' field
	for _, v := range types {
		if _, ok := fieldNames[v.Name()]; ok {
			rowTypes = append(rowTypes, fieldNames[v.Name()])
		} else {
			rowTypes = append(rowTypes, new(sql.RawBytes))
		}
	}

	// read row, and append to result
	for rows.Next() {
		err := rows.Scan(rowTypes...)
		if err != nil {
			return err
		}
		result.Set(reflect.Append(result, elemByValue.Elem()))
	}
	return nil
}

func QueryByTX(db *sql.Tx, objectModel interface{}, query string, args ...interface{}) error {

	// assume objectModel type is *[]struct
	tp := reflect.TypeOf(objectModel)
	kd := tp.Kind()
	switch kd {
	case reflect.Ptr:
		if tp.Elem().Kind() != reflect.Slice {
			return isNotPtrOfStructTypeErr
		}
		if tp.Elem().Elem().Kind() != reflect.Struct {
			return isNotPtrOfStructTypeErr
		}
	default:
		return isNotPtrOfStructTypeErr
	}

	// get value of slice
	result := reflect.ValueOf(objectModel).Elem()

	// get type of struct
	elemByType := reflect.TypeOf(objectModel).Elem().Elem()

	// check field count
	if elemByType.NumField() == 0 {
		return noFeildStructErr
	}

	// cache type info
	typeMark := elemByType.PkgPath() + "." + elemByType.Name()
	cacheTypeInfoInst, has := cacheMap.LoadOrStore(typeMark, &cacheTypeInfo{elemByType: elemByType, ch: make(chan struct{})})
	real := cacheTypeInfoInst.(*cacheTypeInfo)
	if !has {
		fieldNames := make(map[string]interface{})
		for i := 0; i < elemByType.NumField(); i++ {
			if elemByType.Field(i).Type.Kind() == reflect.Struct {
				close(real.ch)
				cacheMap.Delete(typeMark)
				return fieldNotBeStructErr
			}

			if elemByType.Field(i).Anonymous {
				close(real.ch)
				cacheMap.Delete(typeMark)
				return fieldNotBeAnonymous
			}

			tag := elemByType.Field(i).Tag.Get("sql")
			if tag == "" {
				close(real.ch)
				cacheMap.Delete(typeMark)
				return noSQLTagErr
			}

			fieldNames[tag+split+fmt.Sprintf("%d", i)] = nil
		}
		real.fieldNames = fieldNames
		close(real.ch)
	} else {
		<-real.ch
	}

	// alloc type with cache type info
	elemByValue := reflect.New(elemByType)
	fieldNames := make(map[string]interface{})
	for k, _ := range real.fieldNames {
		fieldKey := strings.Split(k, split)
		index, _ := strconv.Atoi(fieldKey[1])
		fieldNames[fieldKey[0]] = elemByValue.Elem().Field(index).Addr().Interface()
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	// check dup column
	types, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	rowTypes := make([]interface{}, 0)
	dupColCheck := map[string]struct{}{}
	for _, v := range types {
		if _, ok := dupColCheck[v.Name()]; ok {
			return colDupErr
		}
		dupColCheck[v.Name()] = struct{}{}
	}

	// map tag 'sql' field
	for _, v := range types {
		if _, ok := fieldNames[v.Name()]; ok {
			rowTypes = append(rowTypes, fieldNames[v.Name()])
		} else {
			rowTypes = append(rowTypes, new(sql.RawBytes))
		}
	}

	// read row, and append to result
	for rows.Next() {
		err := rows.Scan(rowTypes...)
		if err != nil {
			return err
		}
		result.Set(reflect.Append(result, elemByValue.Elem()))
	}
	return nil
}

func Exec(db *sql.DB, query string, args ...interface{}) error {
	_, err := db.Exec(query, args...)
	if err != nil {
		return err
	}
	return nil
}

func ExecByTX(db *sql.Tx, query string, args ...interface{}) error {
	_, err := db.Exec(query, args...)
	if err != nil {
		return err
	}
	return nil
}
