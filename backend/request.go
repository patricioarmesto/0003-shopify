package main

// request.go evita que reserva y checkout tengan dos structs distintos con los
// mismos tres campos. Si el día de mañana se agrega un campo, hay un solo lugar
// que cambiar y ambos endpoints siguen interpretando el mismo JSON.

// CartRequest es el payload compartido por la reserva y el checkout.
type CartRequest struct {
	UserID    string `json:"user_id"`
	ProductID int    `json:"product_id"`
	Qty       int    `json:"qty"`
}

func (r CartRequest) validationError() string {
	if r.UserID == "" || r.ProductID <= 0 || r.Qty <= 0 {
		return "user_id, product_id and qty are required"
	}
	return ""
}

// cartRef junta los dos valores que identifican una reserva. Antes viajaban
// sueltos por cuatro firmas distintas; agruparlos evita mezclar userID con
// productID al pasarlos de un archivo a otro.
type cartRef struct {
	userID    string
	productID int
}

func (r CartRequest) ref() cartRef {
	return cartRef{userID: r.UserID, productID: r.ProductID}
}
