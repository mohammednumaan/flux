package utils

import (
	"encoding/json"
	"net/http"

	requestTypes "github.com/mohammednumaan/flux/internal/types"
)

func SendRegisterHTTPResponse(w http.ResponseWriter, message string) {
	encoder := json.NewEncoder(w)

	registerResponse := requestTypes.RegisterResponse{
		Success: true,
		Message: message,
	}

	err := encoder.Encode(registerResponse)
	if err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}
