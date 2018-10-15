@echo off

java -cp %MARMOT_CLIENT_HOME%/bin/marmot.client.jar ^
marmot.geo.command.RemoteImportGeoJsonMain ^
-host %MARMOT_HOST% -port %MARMOT_PORT% %*