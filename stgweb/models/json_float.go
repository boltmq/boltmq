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
