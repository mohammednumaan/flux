package utils

import (
	"encoding/json"
	"net/http"
)

type RegisterResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func SendRegisterHTTPResponse(w http.ResponseWriter, message string) {
	encoder := json.NewEncoder(w)
	err := encoder.Encode(RegisterResponse{
		Status:  "success",
		Message: message,
	})

	if err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}
