# postos-desbravador

rsrc -arch amd64 -ico touchsistemas.ico -manifest postos.manifest -o rsrc.syso

go build -ldflags="-H windowsgui" -o touchsistemas-desbravador.exe
