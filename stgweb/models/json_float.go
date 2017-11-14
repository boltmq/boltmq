package models

import (
	"github.com/shopspring/decimal"
)

const (
	two = 2 // 小数点后保留2位
)

// JSONFloat 统计类
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/7/19
type JSONFloat float64

// MarshalJSON 用于结构体序列化为float64类型值(小数点后面保留2位数字, 同时将其转化为字符串)
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/7/19
func (p JSONFloat) MarshalJSON() ([]byte, error) {
	return decimal.NewFromFloat(float64(p)).Truncate(int32(two)).MarshalJSON()
}

// AccurateJSONFloat 校验精度，小数点后保留4位
//
// 示例:	AccurateJSONFloat(1234.56789)  // 1234.5678
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/8/15
func (p JSONFloat) AccurateJSONFloat() float64 {
	data := decimal.NewFromFloat(float64(p))
	return AccurateDecimal(data, two)
}

// AccurateDecimal 校验精度，小数点后保留指定位数
//
// 示例:	AccurateDecimal(1234.56789, 2)  // 1234.56
//
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/8/15
func AccurateDecimal(value decimal.Decimal, precision int) float64 {
	if precision < 0 {
		precision = 0
	}
	data := value.Truncate(int32(precision))
	f, _ := data.Float64()
	return f
}
