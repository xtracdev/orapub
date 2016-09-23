build:
	export PKG_CONFIG_PATH=$GOPATH/src/github.com/xtraclabs/orapub/pkgconfig/
	go get github.com/rjeczalik/pkgconfig/cmd/pkg-config
	go get -u github.com/mattn/go-oci8
	go get github.com/Sirupsen/logrus
	go get github.com/xtracdev/goes
	go get github.com/gucumber/gucumber/cmd/gucumber
	go get github.com/golang/protobuf/proto
	go get github.com/stretchr/testify/assert
	go get github.com/xtracdev/oraeventstore
	go get github.com/xtracdev/oraconn
	gucumber
