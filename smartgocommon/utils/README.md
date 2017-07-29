# 工具库

### 方法

* ParseConfig(cfg string, config interface{}) error 解析json文件到config结构体
* MaptoStruct(data map[string]interface{}, result interface{}) error map转换对象
* Get(requestURL string, params url.Values, body interface{}) error Get http请求中取数据
* Post(requestURL string, params string, body interface{}) error Post http请求中取数据
* RecoveredFn(cbs ...func()) panic回复方法，用于出现panic捕获。
* UUID() string 返回标准uuid
* CUID() string 返回去除`-`的uuid
* ValidateStruct(obj interface{}) error 结构体验证
* ValidateVar(field string, tag string) (bool, error) 变量验证

