package namesrv

type RegisterBrokerResponseHeader struct {
	HaServerAddr string
	MasterAddr string
}


func(self *RegisterBrokerResponseHeader) CheckFields()  {

}