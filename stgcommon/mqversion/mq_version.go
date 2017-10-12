package mqversion

const (
	CurrentVersion = V3_2_6 // TODO 每次发布版本都要修改此处版本号
)

const (
	V3_0_0_SNAPSHOT = iota
	V3_0_0_ALPHA1
	V3_0_0_BETA1
	V3_0_0_BETA2
	V3_0_0_BETA3
	V3_0_0_BETA4
	V3_0_0_BETA5
	V3_0_0_BETA6_SNAPSHOT
	V3_0_0_BETA6
	V3_0_0_BETA7_SNAPSHOT
	V3_0_0_BETA7
	V3_0_0_BETA8_SNAPSHOT
	V3_0_0_BETA8
	V3_0_0_BETA9_SNAPSHOT
	V3_0_0_BETA9
	V3_0_0_FINAL
	V3_0_1_SNAPSHOT
	V3_0_1
	V3_0_2_SNAPSHOT
	V3_0_2
	V3_0_3_SNAPSHOT
	V3_0_3
	V3_0_4_SNAPSHOT
	V3_0_4
	V3_0_5_SNAPSHOT
	V3_0_5
	V3_0_6_SNAPSHOT
	V3_0_6
	V3_0_7_SNAPSHOT
	V3_0_7
	V3_0_8_SNAPSHOT
	V3_0_8
	V3_0_9_SNAPSHOT
	V3_0_9
	V3_0_10_SNAPSHOT
	V3_0_10
	V3_0_11_SNAPSHOT
	V3_0_11
	V3_0_12_SNAPSHOT
	V3_0_12
	V3_0_13_SNAPSHOT
	V3_0_13
	V3_0_14_SNAPSHOT
	V3_0_14
	V3_0_15_SNAPSHOT
	V3_0_15
	V3_1_0_SNAPSHOT
	V3_1_0
	V3_1_1_SNAPSHOT
	V3_1_1
	V3_1_2_SNAPSHOT
	V3_1_2
	V3_1_3_SNAPSHOT
	V3_1_3
	V3_1_4_SNAPSHOT
	V3_1_4
	V3_1_5_SNAPSHOT
	V3_1_5
	V3_1_6_SNAPSHOT
	V3_1_6
	V3_1_7_SNAPSHOT
	V3_1_7
	V3_1_8_SNAPSHOT
	V3_1_8
	V3_1_9_SNAPSHOT
	V3_1_9
	V3_2_0_SNAPSHOT
	V3_2_0
	V3_2_1_SNAPSHOT
	V3_2_1
	V3_2_2_SNAPSHOT
	V3_2_2
	V3_2_3_SNAPSHOT
	V3_2_3
	V3_2_4_SNAPSHOT
	V3_2_4
	V3_2_5_SNAPSHOT
	V3_2_5
	V3_2_6_SNAPSHOT
	V3_2_6
	V3_2_7_SNAPSHOT
	V3_2_7
	V3_2_8_SNAPSHOT
	V3_2_8
	V3_2_9_SNAPSHOT
	V3_2_9
	V3_3_1_SNAPSHOT
	V3_3_1
	V3_3_2_SNAPSHOT
	V3_3_2
	V3_3_3_SNAPSHOT
	V3_3_3
	V3_3_4_SNAPSHOT
	V3_3_4
	V3_3_5_SNAPSHOT
	V3_3_5
	V3_3_6_SNAPSHOT
	V3_3_6
	V3_3_7_SNAPSHOT
	V3_3_7
	V3_3_8_SNAPSHOT
	V3_3_8
	V3_3_9_SNAPSHOT
	V3_3_9
	V3_4_1_SNAPSHOT
	V3_4_1
	V3_4_2_SNAPSHOT
	V3_4_2
	V3_4_3_SNAPSHOT
	V3_4_3
	V3_4_4_SNAPSHOT
	V3_4_4
	V3_4_5_SNAPSHOT
	V3_4_5
	V3_4_6_SNAPSHOT
	V3_4_6
	V3_4_7_SNAPSHOT
	V3_4_7
	V3_4_8_SNAPSHOT
	V3_4_8
	V3_4_9_SNAPSHOT
	V3_4_9
	V3_5_1_SNAPSHOT
	V3_5_1
	V3_5_2_SNAPSHOT
	V3_5_2
	V3_5_3_SNAPSHOT
	V3_5_3
	V3_5_4_SNAPSHOT
	V3_5_4
	V3_5_5_SNAPSHOT
	V3_5_5
	V3_5_6_SNAPSHOT
	V3_5_6
	V3_5_7_SNAPSHOT
	V3_5_7
	V3_5_8_SNAPSHOT
	V3_5_8
	V3_5_9_SNAPSHOT
	V3_5_9
)

// versionTable 获取MQ版本号对应表
// 	key:版本标识(枚举的ordinal值), value:版本描述信息(枚举的name值)
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
var versionTable = map[int]string{
	0:   "V3_0_0_SNAPSHOT",
	1:   "V3_0_0_ALPHA1",
	2:   "V3_0_0_BETA1",
	3:   "V3_0_0_BETA2",
	4:   "V3_0_0_BETA3",
	5:   "V3_0_0_BETA4",
	6:   "V3_0_0_BETA5",
	7:   "V3_0_0_BETA6_SNAPSHOT",
	8:   "V3_0_0_BETA6",
	9:   "V3_0_0_BETA7_SNAPSHOT",
	10:  "V3_0_0_BETA7",
	11:  "V3_0_0_BETA8_SNAPSHOT",
	12:  "V3_0_0_BETA8",
	13:  "V3_0_0_BETA9_SNAPSHOT",
	14:  "V3_0_0_BETA9",
	15:  "V3_0_0_FINAL",
	16:  "V3_0_1_SNAPSHOT",
	17:  "V3_0_1",
	18:  "V3_0_2_SNAPSHOT",
	19:  "V3_0_2",
	20:  "V3_0_3_SNAPSHOT",
	21:  "V3_0_3",
	22:  "V3_0_4_SNAPSHOT",
	23:  "V3_0_4",
	24:  "V3_0_5_SNAPSHOT",
	25:  "V3_0_5",
	26:  "V3_0_6_SNAPSHOT",
	27:  "V3_0_6",
	28:  "V3_0_7_SNAPSHOT",
	29:  "V3_0_7",
	30:  "V3_0_8_SNAPSHOT",
	31:  "V3_0_8",
	32:  "V3_0_9_SNAPSHOT",
	33:  "V3_0_9",
	34:  "V3_0_10_SNAPSHOT",
	35:  "V3_0_10",
	36:  "V3_0_11_SNAPSHOT",
	37:  "V3_0_11",
	38:  "V3_0_12_SNAPSHOT",
	39:  "V3_0_12",
	40:  "V3_0_13_SNAPSHOT",
	41:  "V3_0_13",
	42:  "V3_0_14_SNAPSHOT",
	43:  "V3_0_14",
	44:  "V3_0_15_SNAPSHOT",
	45:  "V3_0_15",
	46:  "V3_1_0_SNAPSHOT",
	47:  "V3_1_0",
	48:  "V3_1_1_SNAPSHOT",
	49:  "V3_1_1",
	50:  "V3_1_2_SNAPSHOT",
	51:  "V3_1_2",
	52:  "V3_1_3_SNAPSHOT",
	53:  "V3_1_3",
	54:  "V3_1_4_SNAPSHOT",
	55:  "V3_1_4",
	56:  "V3_1_5_SNAPSHOT",
	57:  "V3_1_5",
	58:  "V3_1_6_SNAPSHOT",
	59:  "V3_1_6",
	60:  "V3_1_7_SNAPSHOT",
	61:  "V3_1_7",
	62:  "V3_1_8_SNAPSHOT",
	63:  "V3_1_8",
	64:  "V3_1_9_SNAPSHOT",
	65:  "V3_1_9",
	66:  "V3_2_0_SNAPSHOT",
	67:  "V3_2_0",
	68:  "V3_2_1_SNAPSHOT",
	69:  "V3_2_1",
	70:  "V3_2_2_SNAPSHOT",
	71:  "V3_2_2",
	72:  "V3_2_3_SNAPSHOT",
	73:  "V3_2_3",
	74:  "V3_2_4_SNAPSHOT",
	75:  "V3_2_4",
	76:  "V3_2_5_SNAPSHOT",
	77:  "V3_2_5",
	78:  "V3_2_6_SNAPSHOT",
	79:  "V3_2_6",
	80:  "V3_2_7_SNAPSHOT",
	81:  "V3_2_7",
	82:  "V3_2_8_SNAPSHOT",
	83:  "V3_2_8",
	84:  "V3_2_9_SNAPSHOT",
	85:  "V3_2_9",
	86:  "V3_3_1_SNAPSHOT",
	87:  "V3_3_1",
	88:  "V3_3_2_SNAPSHOT",
	89:  "V3_3_2",
	90:  "V3_3_3_SNAPSHOT",
	91:  "V3_3_3",
	92:  "V3_3_4_SNAPSHOT",
	93:  "V3_3_4",
	94:  "V3_3_5_SNAPSHOT",
	95:  "V3_3_5",
	96:  "V3_3_6_SNAPSHOT",
	97:  "V3_3_6",
	98:  "V3_3_7_SNAPSHOT",
	99:  "V3_3_7",
	100: "V3_3_8_SNAPSHOT",
	101: "V3_3_8",
	102: "V3_3_9_SNAPSHOT",
	103: "V3_3_9",
	104: "V3_4_1_SNAPSHOT",
	105: "V3_4_1",
	106: "V3_4_2_SNAPSHOT",
	107: "V3_4_2",
	108: "V3_4_3_SNAPSHOT",
	109: "V3_4_3",
	110: "V3_4_4_SNAPSHOT",
	111: "V3_4_4",
	112: "V3_4_5_SNAPSHOT",
	113: "V3_4_5",
	114: "V3_4_6_SNAPSHOT",
	115: "V3_4_6",
	116: "V3_4_7_SNAPSHOT",
	117: "V3_4_7",
	118: "V3_4_8_SNAPSHOT",
	119: "V3_4_8",
	120: "V3_4_9_SNAPSHOT",
	121: "V3_4_9",
	122: "V3_5_1_SNAPSHOT",
	123: "V3_5_1",
	124: "V3_5_2_SNAPSHOT",
	125: "V3_5_2",
	126: "V3_5_3_SNAPSHOT",
	127: "V3_5_3",
	128: "V3_5_4_SNAPSHOT",
	129: "V3_5_4",
	130: "V3_5_5_SNAPSHOT",
	131: "V3_5_5",
	132: "V3_5_6_SNAPSHOT",
	133: "V3_5_6",
	134: "V3_5_7_SNAPSHOT",
	135: "V3_5_7",
	136: "V3_5_8_SNAPSHOT",
	137: "V3_5_8",
	138: "V3_5_9_SNAPSHOT",
	139: "V3_5_9",
}

// GetVersionDesc 通过MQ版本号查找对应的版本描述
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func GetVersionDesc(versionCode int) (versionDesc string) {
	if versionDesc, ok := versionTable[versionCode]; ok {
		return versionDesc
	}

	return "HigherVersion"
}

// Value2Version 获取对应MQ版本号
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func Value2Version(versionCode int32) int {
	return int(versionCode) // 可以直接使用
}
