package types

type RegisterRequest struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type RegisterResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}
