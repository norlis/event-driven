package domain

import "time"

type Event[T any] struct {
	Header Header `json:"encabezado"`
	Body   *T     `json:"cuerpo"`
}

type Header struct {
	CorrelationId   string `json:"idCorrelacion"`
	EventId         string `json:"codEvento"`
	EventName       string `json:"nombreEvento"`
	User            string `json:"usuario"`
	ApplicationId   string `json:"aplicacionOrigen"`
	TransactionDate string `json:"fechaTransaccion"`
	CommercialUser  string `json:"usuarioComercial"`
}

type Account struct {
	PersonType         string             `json:"tipoPersona"`
	IdentificationType string             `json:"tipoIdentificacion" validate:"required"`
	IdentificationId   string             `json:"numeroIdentificacion" validate:"required"`
	FinancialAccounts  []FinancialAccount `json:"cuentasFinancieras" validate:"required,gt=0,dive,required"`
	Roles              []Role             `json:"rol,omitempty" `
	BusinessLine       []BusinessLine     `json:"lineasNegocio,omitempty" `
	//Type               string             `json:"tipo"`
	//Number             string             `json:"numero"`
}

type SyncAccount struct {
	PersonType         string             `json:"tipoPersona"`
	IdentificationType string             `json:"tipoIdentificacion" validate:"required"`
	IdentificationId   string             `json:"numeroIdentificacion" validate:"required"`
	FinancialAccounts  []FinancialAccount `json:"cuentasFinancieras" validate:"required,gt=0,dive,required"`
	BusinessLine       []BusinessLine     `json:"lineasNegocio" validate:"required,gt=0,dive,required"`
}

type FinancialAccount struct {
	Code             string       `json:"codigo,omitempty"`
	AccountNumber    string       `json:"numeroCuenta" validate:"required"`
	AccountStatus    string       `json:"estadoCuenta" validate:"required"`
	RegistrationDate time.Time    `json:"fechaInscripcion,omitempty"`
	Bank             *Bank        `json:"banco" validate:"required"`
	AccountType      *AccountType `json:"tipoCuenta" validate:"required"`
	Transversal      bool         `json:"transversal,omitempty"`
}

type Bank struct {
	Code string `json:"codigo" validate:"required"`
	Name string `json:"nombre,omitempty"`
}

type AccountType struct {
	Code string `json:"codigo" validate:"required"`
	Name string `json:"nombre"`
}

type Role struct {
	Code string `json:"codigo"`
	Name string `json:"nombre"`
}

type BusinessLine struct {
	Code   string `json:"codigo"`
	Name   string `json:"nombre"`
	Status string `json:"estado"`
}

const BusinessLineStatusActive = "Activo"
