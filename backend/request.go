package main

// CartRequest is the shared payload for reserving and checking out a cart line.
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

// cartRef bundles the two values that identify one reservation everywhere.
type cartRef struct {
	userID    string
	productID int
}

func (r CartRequest) ref() cartRef {
	return cartRef{userID: r.UserID, productID: r.ProductID}
}
