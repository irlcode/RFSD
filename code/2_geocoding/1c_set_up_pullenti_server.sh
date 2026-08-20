# 1) Install dotnet: https://dotnet.microsoft.com/en-us/download/dotnet
# 2) Obtain GARIndex (ToDo: explain how) 
# 3) Run the Pullenti server with
cd pullenti
dotnet "AddressServer/Address.Server.dll" -- -gar "YOUR_DIR/GARIndex/INDEX_VERSION/GarAll"
