package structs

type JmxBean struct {
	Beans []map[string]interface{} `json:"beans"`
}
