package mqversion

import (
	"sync"
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

var (
	once         sync.Once
	versionTable map[int]string
)

func getVersionTable() map[int]string {
	once.Do(func() {
		versionTable = make(map[int]string) // key:版本标识(枚举的ordinal值), value:版本描述信息(枚举的name值)
		versionTable[0] = "V3_0_0_SNAPSHOT"
		versionTable[1] = "V3_0_0_ALPHA1"
		versionTable[2] = "V3_0_0_BETA1"
		versionTable[3] = "V3_0_0_BETA2"
		versionTable[4] = "V3_0_0_BETA3"
		versionTable[5] = "V3_0_0_BETA4"
		versionTable[6] = "V3_0_0_BETA5"
		versionTable[7] = "V3_0_0_BETA6_SNAPSHOT"
		versionTable[8] = "V3_0_0_BETA6"
		versionTable[9] = "V3_0_0_BETA7_SNAPSHOT"
		versionTable[10] = "V3_0_0_BETA7"
		versionTable[11] = "V3_0_0_BETA8_SNAPSHOT"
		versionTable[12] = "V3_0_0_BETA8"
		versionTable[13] = "V3_0_0_BETA9_SNAPSHOT"
		versionTable[14] = "V3_0_0_BETA9"
		versionTable[15] = "V3_0_0_FINAL"
		versionTable[16] = "V3_0_1_SNAPSHOT"
		versionTable[17] = "V3_0_1"
		versionTable[18] = "V3_0_2_SNAPSHOT"
		versionTable[19] = "V3_0_2"
		versionTable[20] = "V3_0_3_SNAPSHOT"
		versionTable[21] = "V3_0_3"
		versionTable[22] = "V3_0_4_SNAPSHOT"
		versionTable[23] = "V3_0_4"
		versionTable[24] = "V3_0_5_SNAPSHOT"
		versionTable[25] = "V3_0_5"
		versionTable[26] = "V3_0_6_SNAPSHOT"
		versionTable[27] = "V3_0_6"
		versionTable[28] = "V3_0_7_SNAPSHOT"
		versionTable[29] = "V3_0_7"
		versionTable[30] = "V3_0_8_SNAPSHOT"
		versionTable[31] = "V3_0_8"
		versionTable[32] = "V3_0_9_SNAPSHOT"
		versionTable[33] = "V3_0_9"
		versionTable[34] = "V3_0_10_SNAPSHOT"
		versionTable[35] = "V3_0_10"
		versionTable[36] = "V3_0_11_SNAPSHOT"
		versionTable[37] = "V3_0_11"
		versionTable[38] = "V3_0_12_SNAPSHOT"
		versionTable[39] = "V3_0_12"
		versionTable[40] = "V3_0_13_SNAPSHOT"
		versionTable[41] = "V3_0_13"
		versionTable[42] = "V3_0_14_SNAPSHOT"
		versionTable[43] = "V3_0_14"
		versionTable[44] = "V3_0_15_SNAPSHOT"
		versionTable[45] = "V3_0_15"
		versionTable[46] = "V3_1_0_SNAPSHOT"
		versionTable[47] = "V3_1_0"
		versionTable[48] = "V3_1_1_SNAPSHOT"
		versionTable[49] = "V3_1_1"
		versionTable[50] = "V3_1_2_SNAPSHOT"
		versionTable[51] = "V3_1_2"
		versionTable[52] = "V3_1_3_SNAPSHOT"
		versionTable[53] = "V3_1_3"
		versionTable[54] = "V3_1_4_SNAPSHOT"
		versionTable[55] = "V3_1_4"
		versionTable[56] = "V3_1_5_SNAPSHOT"
		versionTable[57] = "V3_1_5"
		versionTable[58] = "V3_1_6_SNAPSHOT"
		versionTable[59] = "V3_1_6"
		versionTable[60] = "V3_1_7_SNAPSHOT"
		versionTable[61] = "V3_1_7"
		versionTable[62] = "V3_1_8_SNAPSHOT"
		versionTable[63] = "V3_1_8"
		versionTable[64] = "V3_1_9_SNAPSHOT"
		versionTable[65] = "V3_1_9"
		versionTable[66] = "V3_2_0_SNAPSHOT"
		versionTable[67] = "V3_2_0"
		versionTable[68] = "V3_2_1_SNAPSHOT"
		versionTable[69] = "V3_2_1"
		versionTable[70] = "V3_2_2_SNAPSHOT"
		versionTable[71] = "V3_2_2"
		versionTable[72] = "V3_2_3_SNAPSHOT"
		versionTable[73] = "V3_2_3"
		versionTable[74] = "V3_2_4_SNAPSHOT"
		versionTable[75] = "V3_2_4"
		versionTable[76] = "V3_2_5_SNAPSHOT"
		versionTable[77] = "V3_2_5"
		versionTable[78] = "V3_2_6_SNAPSHOT"
		versionTable[79] = "V3_2_6"
		versionTable[80] = "V3_2_7_SNAPSHOT"
		versionTable[81] = "V3_2_7"
		versionTable[82] = "V3_2_8_SNAPSHOT"
		versionTable[83] = "V3_2_8"
		versionTable[84] = "V3_2_9_SNAPSHOT"
		versionTable[85] = "V3_2_9"
		versionTable[86] = "V3_3_1_SNAPSHOT"
		versionTable[87] = "V3_3_1"
		versionTable[88] = "V3_3_2_SNAPSHOT"
		versionTable[89] = "V3_3_2"
		versionTable[90] = "V3_3_3_SNAPSHOT"
		versionTable[91] = "V3_3_3"
		versionTable[92] = "V3_3_4_SNAPSHOT"
		versionTable[93] = "V3_3_4"
		versionTable[94] = "V3_3_5_SNAPSHOT"
		versionTable[95] = "V3_3_5"
		versionTable[96] = "V3_3_6_SNAPSHOT"
		versionTable[97] = "V3_3_6"
		versionTable[98] = "V3_3_7_SNAPSHOT"
		versionTable[99] = "V3_3_7"
		versionTable[100] = "V3_3_8_SNAPSHOT"
		versionTable[101] = "V3_3_8"
		versionTable[102] = "V3_3_9_SNAPSHOT"
		versionTable[103] = "V3_3_9"
		versionTable[104] = "V3_4_1_SNAPSHOT"
		versionTable[105] = "V3_4_1"
		versionTable[106] = "V3_4_2_SNAPSHOT"
		versionTable[107] = "V3_4_2"
		versionTable[108] = "V3_4_3_SNAPSHOT"
		versionTable[109] = "V3_4_3"
		versionTable[110] = "V3_4_4_SNAPSHOT"
		versionTable[111] = "V3_4_4"
		versionTable[112] = "V3_4_5_SNAPSHOT"
		versionTable[113] = "V3_4_5"
		versionTable[114] = "V3_4_6_SNAPSHOT"
		versionTable[115] = "V3_4_6"
		versionTable[116] = "V3_4_7_SNAPSHOT"
		versionTable[117] = "V3_4_7"
		versionTable[118] = "V3_4_8_SNAPSHOT"
		versionTable[119] = "V3_4_8"
		versionTable[120] = "V3_4_9_SNAPSHOT"
		versionTable[121] = "V3_4_9"
		versionTable[122] = "V3_5_1_SNAPSHOT"
		versionTable[123] = "V3_5_1"
		versionTable[124] = "V3_5_2_SNAPSHOT"
		versionTable[125] = "V3_5_2"
		versionTable[126] = "V3_5_3_SNAPSHOT"
		versionTable[127] = "V3_5_3"
		versionTable[128] = "V3_5_4_SNAPSHOT"
		versionTable[129] = "V3_5_4"
		versionTable[130] = "V3_5_5_SNAPSHOT"
		versionTable[131] = "V3_5_5"
		versionTable[132] = "V3_5_6_SNAPSHOT"
		versionTable[133] = "V3_5_6"
		versionTable[134] = "V3_5_7_SNAPSHOT"
		versionTable[135] = "V3_5_7"
		versionTable[136] = "V3_5_8_SNAPSHOT"
		versionTable[137] = "V3_5_8"
		versionTable[138] = "V3_5_9_SNAPSHOT"
		versionTable[139] = "V3_5_9"
	})

	return versionTable
}
