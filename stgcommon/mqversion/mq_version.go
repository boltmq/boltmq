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
	0:   "v3.0.0.SNAPSHOT",
	1:   "v3.0.0.ALPHA1",
	2:   "v3.0.0.BETA1",
	3:   "v3.0.0.BETA2",
	4:   "v3.0.0.BETA3",
	5:   "v3.0.0.BETA4",
	6:   "v3.0.0.BETA5",
	7:   "v3.0.0.BETA6.SNAPSHOT",
	8:   "v3.0.0.BETA6",
	9:   "v3.0.0.BETA7.SNAPSHOT",
	10:  "v3.0.0.BETA7",
	11:  "v3.0.0.BETA8.SNAPSHOT",
	12:  "v3.0.0.BETA8",
	13:  "v3.0.0.BETA9.SNAPSHOT",
	14:  "v3.0.0.BETA9",
	15:  "v3.0.0.FINAL",
	16:  "v3.0.1.SNAPSHOT",
	17:  "v3.0.1",
	18:  "v3.0.2.SNAPSHOT",
	19:  "v3.0.2",
	20:  "v3.0.3.SNAPSHOT",
	21:  "v3.0.3",
	22:  "v3.0.4.SNAPSHOT",
	23:  "v3.0.4",
	24:  "v3.0.5.SNAPSHOT",
	25:  "v3.0.5",
	26:  "v3.0.6.SNAPSHOT",
	27:  "v3.0.6",
	28:  "v3.0.7.SNAPSHOT",
	29:  "v3.0.7",
	30:  "v3.0.8.SNAPSHOT",
	31:  "v3.0.8",
	32:  "v3.0.9.SNAPSHOT",
	33:  "v3.0.9",
	34:  "v3.0.10.SNAPSHOT",
	35:  "v3.0.10",
	36:  "v3.0.11.SNAPSHOT",
	37:  "v3.0.11",
	38:  "v3.0.12.SNAPSHOT",
	39:  "v3.0.12",
	40:  "v3.0.13.SNAPSHOT",
	41:  "v3.0.13",
	42:  "v3.0.14.SNAPSHOT",
	43:  "v3.0.14",
	44:  "v3.0.15.SNAPSHOT",
	45:  "v3.0.15",
	46:  "v3.1.0.SNAPSHOT",
	47:  "v3.1.0",
	48:  "v3.1.1.SNAPSHOT",
	49:  "v3.1.1",
	50:  "v3.1.2.SNAPSHOT",
	51:  "v3.1.2",
	52:  "v3.1.3.SNAPSHOT",
	53:  "v3.1.3",
	54:  "v3.1.4.SNAPSHOT",
	55:  "v3.1.4",
	56:  "v3.1.5.SNAPSHOT",
	57:  "v3.1.5",
	58:  "v3.1.6.SNAPSHOT",
	59:  "v3.1.6",
	60:  "v3.1.7.SNAPSHOT",
	61:  "v3.1.7",
	62:  "v3.1.8.SNAPSHOT",
	63:  "v3.1.8",
	64:  "v3.1.9.SNAPSHOT",
	65:  "v3.1.9",
	66:  "v3.2.0.SNAPSHOT",
	67:  "v3.2.0",
	68:  "v3.2.1.SNAPSHOT",
	69:  "v3.2.1",
	70:  "v3.2.2.SNAPSHOT",
	71:  "v3.2.2",
	72:  "v3.2.3.SNAPSHOT",
	73:  "v3.2.3",
	74:  "v3.2.4.SNAPSHOT",
	75:  "v3.2.4",
	76:  "v3.2.5.SNAPSHOT",
	77:  "v3.2.5",
	78:  "v3.2.6.SNAPSHOT",
	79:  "v3.2.6",
	80:  "v3.2.7.SNAPSHOT",
	81:  "v3.2.7",
	82:  "v3.2.8.SNAPSHOT",
	83:  "v3.2.8",
	84:  "v3.2.9.SNAPSHOT",
	85:  "v3.2.9",
	86:  "v3.3.1.SNAPSHOT",
	87:  "v3.3.1",
	88:  "v3.3.2.SNAPSHOT",
	89:  "v3.3.2",
	90:  "v3.3.3.SNAPSHOT",
	91:  "v3.3.3",
	92:  "v3.3.4.SNAPSHOT",
	93:  "v3.3.4",
	94:  "v3.3.5.SNAPSHOT",
	95:  "v3.3.5",
	96:  "v3.3.6.SNAPSHOT",
	97:  "v3.3.6",
	98:  "v3.3.7.SNAPSHOT",
	99:  "v3.3.7",
	100: "v3.3.8.SNAPSHOT",
	101: "v3.3.8",
	102: "v3.3.9.SNAPSHOT",
	103: "v3.3.9",
	104: "v3.4.1.SNAPSHOT",
	105: "v3.4.1",
	106: "v3.4.2.SNAPSHOT",
	107: "v3.4.2",
	108: "v3.4.3.SNAPSHOT",
	109: "v3.4.3",
	110: "v3.4.4.SNAPSHOT",
	111: "v3.4.4",
	112: "v3.4.5.SNAPSHOT",
	113: "v3.4.5",
	114: "v3.4.6.SNAPSHOT",
	115: "v3.4.6",
	116: "v3.4.7.SNAPSHOT",
	117: "v3.4.7",
	118: "v3.4.8.SNAPSHOT",
	119: "v3.4.8",
	120: "v3.4.9.SNAPSHOT",
	121: "v3.4.9",
	122: "v3.5.1.SNAPSHOT",
	123: "v3.5.1",
	124: "v3.5.2.SNAPSHOT",
	125: "v3.5.2",
	126: "v3.5.3.SNAPSHOT",
	127: "v3.5.3",
	128: "v3.5.4.SNAPSHOT",
	129: "v3.5.4",
	130: "v3.5.5.SNAPSHOT",
	131: "v3.5.5",
	132: "v3.5.6.SNAPSHOT",
	133: "v3.5.6",
	134: "v3.5.7.SNAPSHOT",
	135: "v3.5.7",
	136: "v3.5.8.SNAPSHOT",
	137: "v3.5.8",
	138: "v3.5.9.SNAPSHOT",
	139: "v3.5.9",
}

// GetCurrentDesc 通过当前MQ版本描述
// Author: tianyuliang, <tianyuliang@gome.com.cn>
// Since: 2017/9/6
func GetCurrentDesc() string {
	return GetVersionDesc(CurrentVersion)
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
