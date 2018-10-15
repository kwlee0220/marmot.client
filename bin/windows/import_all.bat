
############################################################################################
##### 주요 구역들
############################################################################################
mc_import_shapefile '%MARMOT_DATA%/행자부/법정구역_5179/시군구' -dataset 구역/시군구 -shp_srid EPSG:5179 -srid EPSG:5186 -charset euc-kr
mc_import_shapefile %MARMOT_DATA%/행자부/법정구역_5179/시군구 -dataset 구역/시군구 -shp_srid EPSG:5179 -srid EPSG:5186 -charset euc-kr
mc_cluster_dataset 구역/시군구
mc_import_shapefile %MARMOT_DATA%/행자부/법정구역_5179/읍면동 -dataset 구역/읍면동 -shp_srid EPSG:5179 -srid EPSG:5186 -charset euc-kr
mc_cluster_dataset 구역/읍면동
mc_import_shapefile %MARMOT_DATA%/행자부/법정구역_5179/리 -dataset 구역/리 -shp_srid EPSG:5179 -srid EPSG:5186 -charset euc-kr
mc_import_shapefile %MARMOT_DATA%/행자부/기초구역_5179 -dataset 구역/기초구역 -shp_srid EPSG:5179 -srid EPSG:5186 -charset euc-kr
mc_cluster_dataset 구역/기초구역
mc_import_shapefile %MARMOT_DATA%/사업단_자료/통계청집계구 -dataset 구역/집계구 -srid EPSG:5186 -charset euc-kr
mc_cluster_dataset 구역/집계구
mc_import_shapefile %MARMOT_DATA%/사업단_자료/지오비전/집계구 -dataset 구역/지오비전_집계구 -srid EPSG:5186
mc_cluster_dataset 구역/지오비전_집계구
##### 연속지적도_2017
mc_import_shapefile %MARMOT_DATA%/공공데이터포털/연속지적도_2017 -dataset 구역/연속지적도 -srid EPSG:5186 -charset euc-kr
mc_cluster_dataset 구역/연속지적도 -workers 29
##### 소방서관할구역
mc_import_shapefile %MARMOT_DATA%/사업단_자료/소방서관할구역 -dataset 구역/소방서관할구역 -srid EPSG:5186 -charset utf-8


############################################################################################
############################################################################################
##### 도로명/지번 주소 관련 정보
############################################################################################
############################################################################################
hadoop fs -mkdir data/도로명주소
hadoop fs -copyFromLocal $MARMOT_DATA/행자부/도로명주소/주소 data/도로명주소/주소
mc_bind_dataset -type csv data/도로명주소/주소 -dataset 주소/주소

hadoop fs -copyFromLocal $MARMOT_DATA/행자부/도로명주소/도로명코드 data/도로명주소
mc_bind_dataset -type csv data/도로명주소/도로명코드 -dataset 주소/도로명코드

hadoop fs -copyFromLocal $MARMOT_DATA/행자부/도로명주소/지번 data/도로명주소
mc_bind_dataset -type csv data/도로명주소/지번 -dataset 주소/지번

hadoop fs -copyFromLocal $MARMOT_DATA/행자부/도로명주소/부가정보 data/도로명주소
mc_bind_dataset -type csv data/도로명주소/부가정보 -dataset 주소/부가정보

hadoop fs -copyFromLocal $MARMOT_DATA/행자부/도로명주소/건물 data/도로명주소
mc_bind_dataset -type csv data/도로명주소/건물 -dataset 주소/건물

hadoop fs -copyFromLocal $MARMOT_DATA/행자부/도로명주소/건물_위치정보 data/도로명주소
mc_bind_dataset -type csv data/도로명주소/건물_위치정보 -dataset 주소/건물POI -geom_col the_geom -srid EPSG:5186
mc_cluster_dataset 주소/건물POI



############################################################################################
############################################################################################
##### 토지 관련 정보
############################################################################################
############################################################################################
hadoop fs -mkdir data/토지
##### 공시지가: 개별, 표준
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/표준공시지가 data/토지
mc_bind_dataset data/토지/표준공시지가 -dataset 토지/표준공시지가 -type csv
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/개별공시지가 data/토지
mc_bind_dataset data/토지/개별공시지가 -dataset 토지/개별공시지가 -type csv
##### 용도지역지구
mc_import_shapefile %MARMOT_DATA%/사업단_자료/용도지역지구 -dataset 토지/용도지역지구 -srid EPSG:5186 -charset euc-kr
##### 토지이용계획
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/토지이용계획 data/토지
mc_bind_dataset data/토지/토지이용계획 -dataset 토지/토지이용계획 -type csv
##### 도시지역
mc_import_shapefile %MARMOT_DATA%/포스웨이브/전국도시 -dataset 토지/도시지역 -srid EPSG:5186 -charset euc-kr


############################################################################################
############################################################################################
##### 건물/주택 정보
############################################################################################
############################################################################################
hadoop fs -mkdir data/주택
mc_import_shapefile %MARMOT_DATA%/공공데이터포털/건물통합정보 -dataset 건물/통합정보 -srid EPSG:5186 -charset euc-kr
mc_cluster_dataset 건물/통합정보 -workers 17
mc_import_shapefile %MARMOT_DATA%/행자부/건물_5179 -dataset 건물/위치 -shp_srid EPSG:5179 -srid EPSG:5186 -charset euc-kr
mc_cluster_dataset 건물/위치 -workers 17
mc_import_shapefile %MARMOT_DATA%/행자부/건물_출입구_5179 -dataset 건물/출입구 -shp_srid EPSG:5179 -srid EPSG:5186 -charset euc-kr
##### 주택가격: 공동, 개별
mc_import_shapefile %MARMOT_DATA%/공공데이터포털/공동주택가격 -dataset 주택/공동주택가격 -srid EPSG:5186 -charset euc-kr
mc_import_shapefile %MARMOT_DATA%/공공데이터포털/개별주택가격 -dataset 주택/개별주택가격 -srid EPSG:5186 -charset euc-kr
##### 아파트 실매매가
hadoop fs -mkdir data/주택/실거래
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/아파트실거래 data/주택/실거래/아파트매매
mc_bind_dataset -type csv data/주택/실거래/아파트매매 -dataset 주택/실거래/아파트매매


############################################################################################
############################################################################################
##### 국민/주민 관련 정보
############################################################################################
############################################################################################
hadoop fs -mkdir data/국민
##### 주민등록인구_2015
hadoop fs -copyFromLocal $MARMOT_DATA/사업단_자료/주민등록인구_2015 data/국민/주민등록인구
mc_bind_dataset data/국민/주민등록인구 -dataset 국민/주민등록인구 -type csv
##### 직장인구
mc_import_shapefile %MARMOT_DATA%/포스웨이브/전국직장인구 -dataset 국민/직장인구 -srid EPSG:5186 -charset euc-kr
##### 지오비전 유동인구
hadoop fs -mkdir data/국민/유동인구
hadoop fs -mkdir data/국민/유동인구/2015
hadoop fs -mkdir data/국민/유동인구/2015/시간대
hadoop fs -copyFromLocal $MARMOT_DATA/사업단_자료/지오비전/유동인구_2015/시간대/*.txt data/국민/유동인구/2015/시간대
hadoop fs -copyFromLocal $MARMOT_DATA/사업단_자료/지오비전/유동인구_2015/시간대/*.xml data/국민/유동인구/2015/시간대
mc_bind_dataset -type csv data/국민/유동인구/2015/시간대 -dataset 국민/유동인구/시간대/2015/
mc_import_shapefile %MARMOT_DATA%/사업단_자료/지오비전/유동인구_2014 -dataset 국민/유동인구/2014/시간대별 -srid EPSG:5186 -charset euc-kr
##### 지오비전 카드매출
hadoop fs -mkdir data/국민/카드매출
hadoop fs -mkdir data/국민/카드매출/2015
hadoop fs -mkdir data/국민/카드매출/2015/시간대
hadoop fs -copyFromLocal $MARMOT_DATA/사업단_자료/지오비전/카드매출/시간대/*.txt data/국민/카드매출/2015/시간대
hadoop fs -copyFromLocal $MARMOT_DATA/사업단_자료/지오비전/카드매출/시간대/*.xml data/국민/카드매출/2015/시간대
mc_bind_dataset -type csv data/국민/카드매출/2015/시간대 -dataset 국민/카드매출/시간대/2015


############################################################################################
############################################################################################
##### 교통 관련 정보
############################################################################################
############################################################################################
hadoop fs -mkdir data/교통
mc_import_shapefile %MARMOT_DATA%/행자부/도로구간_5179 -dataset 교통/도로구간 -shp_srid EPSG:5179 -srid EPSG:5186 -charset euc-kr
##### 도로망_2013
mc_import_shapefile %MARMOT_DATA%/사업단_자료/도로망_2013/링크 -dataset 교통/도로망/링크 -srid EPSG:5186 -charset euc-kr
mc_cluster_dataset 교통/도로망/링크
mc_import_shapefile %MARMOT_DATA%/사업단_자료/도로망_2013/노드 -dataset 교통/도로망/노드 -srid EPSG:5186 -charset euc-kr
#####  철도망_2013
mc_import_shapefile %MARMOT_DATA%/사업단_자료/철도망_2013/철도교차점 -dataset 교통/철도망/노드 -srid EPSG:5186 -charset euc-kr
mc_import_shapefile %MARMOT_DATA%/사업단_자료/철도망_2013/철도중심선 -dataset 교통/철도망/링크 -srid EPSG:5186 -charset euc-kr
##### 지하철
mc_import_shapefile %MARMOT_DATA%/사업단_자료/전국지하철_2015/역사 -dataset 교통/지하철/역사 -srid EPSG:5186 -charset euc-kr
mc_import_shapefile %MARMOT_DATA%/사업단_자료/전국지하철_2015/선로 -dataset 교통/지하철/선로 -srid EPSG:5186 -charset euc-kr
mc_import_shapefile %MARMOT_DATA%/사업단_자료/전국지하철_2015/출입구 -dataset 교통/지하철/출입구 -srid EPSG:5186 -charset euc-kr
mc_import_shapefile %MARMOT_DATA%/포스웨이브/서울지하철역사/역사.shp -dataset 교통/지하철/서울역사 -srid EPSG:5186 -charset euc-kr
mc_cluster_dataset 교통/지하철/서울역사
##### 버스정류장
mc_import_shapefile %MARMOT_DATA%/BizGis/버스정류장_POI_5181 -dataset 교통/버스/정류장 -shp_srid EPSG:5181 -srid EPSG:5186 -charset euc-kr
hadoop fs -mkdir data/교통/버스
hadoop fs -copyFromLocal $MARMOT_DATA/기타/서울버스노선정류장 data/POI
mc_bind_dataset -type csv data/POI/서울버스노선정류장 -dataset 교통/버스/서울정류장  -geom_col the_geom -srid EPSG:5186
##### 나비콜 택시 운행 로그
hadoop fs -mkdir data/로그
hadoop fs -mkdir data/로그/나비콜
hadoop fs -copyFromLocal $MARMOT_DATA/나비콜-택시 data/로그/나비콜
mc_bind_dataset -type csv data/로그/나비콜 -dataset 로그/나비콜/택시로그 -srid EPSG:5186 -geom_col the_geom


############################################################################################
############################################################################################
##### 주요 지형지물들
############################################################################################
############################################################################################
##### 전국하천
mc_import_shapefile %MARMOT_DATA%/사업단_자료/전국하천/국가하천 -dataset 지형지물/하천/국가하천 -srid EPSG:5186 -charset euc-kr
mc_import_shapefile %MARMOT_DATA%/사업단_자료/전국하천/지방1급하천 -dataset 지형지물/하천/지방1급하천 -srid EPSG:5186 -charset euc-kr
mc_import_shapefile %MARMOT_DATA%/사업단_자료/전국하천/지방2급하천 -dataset 지형지물/하천/지방2급하천 -srid EPSG:5186 -charset euc-kr
##### 전국 호수저수지
mc_import_shapefile %MARMOT_DATA%/사업단_자료/호수저수지 -dataset 지형지물/호수저수지 -srid EPSG:5186 -charset euc-kr
##### 산사태 위험등급
mc_import_shapefile %MARMOT_DATA%/사업단_자료/산사태_위험등급지도 -dataset 지형지물/산사태위험등급 -srid EPSG:5186 -charset euc-kr


############################################################################################
############################################################################################
##### 주요 POI들
############################################################################################
############################################################################################
##### 민원행정기관
mc_import_shapefile %MARMOT_DATA%/행자부/민원행정기관 -dataset POI/민원행정기관 -shp_srid EPSG:5186 -srid EPSG:5186 -charset euc-kr
##### 전국 CCTV 설치장소
hadoop fs -rm -r data/POI/전국cctv
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/전국cctv data/POI
mc_bind_dataset -type csv data/POI/전국cctv -dataset POI/전국cctv  -geom_col the_geom -srid EPSG:5186
##### 전국 은행
mc_import_shapefile %MARMOT_DATA%/BizGis/은행_POI_5181 -dataset POI/은행 -shp_srid EPSG:5181 -srid EPSG:5186 -charset euc-kr
##### 주유소
hadoop fs -rm -r data/POI/주유소
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/주유소 data/POI
mc_bind_dataset -type csv data/POI/주유소 -dataset POI/주유소  -geom_col the_geom -srid EPSG:5186
##### 주유소 가격
hadoop fs -rm -r data/POI/주유소 가격
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/주유소_가격 data/POI
mc_bind_dataset -type csv data/POI/주유소_가격 -dataset POI/주유소_가격  -geom_col the_geom -srid EPSG:5186
##### 상가업소
hadoop fs -rm -r data/POI/상가업소
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/상가업소정보_201703 data/POI/상가업소
mc_bind_dataset -type csv data/POI/상가업소 -dataset POI/상가업소 -geom_col the_geom -srid EPSG:5186
##### 공중화장실
hadoop fs -rm -r data/POI/공중화장실
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/공중화장실 data/POI
mc_bind_dataset -type csv data/POI/공중화장실 -dataset POI/공중화장실 -geom_col the_geom -srid EPSG:5186
##### 전국도서관
hadoop fs -rm -r data/POI/전국도서관
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/전국도서관 data/POI
mc_bind_dataset -type csv data/POI/전국도서관 -dataset POI/전국도서관 -geom_col the_geom -srid EPSG:5186
##### 전국도시공원
hadoop fs -rm -r data/POI/전국도시공원
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/전국도시공원 data/POI
mc_bind_dataset -type csv data/POI/전국도시공원 -dataset POI/전국도시공원 -geom_col the_geom -srid EPSG:5186
##### 전국초중등학교
hadoop fs -rm -r data/POI/전국초중등학교
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/전국초중등학교 data/POI
mc_bind_dataset -type csv data/POI/전국초중등학교 -dataset POI/전국초중등학교 -geom_col the_geom -srid EPSG:5186
##### 전국어린이집
hadoop fs -rm -r data/POI/전국어린이집
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/전국어린이집 data/POI
mc_bind_dataset -type csv data/POI/전국어린이집 -dataset POI/전국어린이집 -geom_col the_geom -srid EPSG:5186
##### 병원
hadoop fs -rm -r data/POI/병원
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/병원 data/POI
mc_bind_dataset -type csv data/POI/병원 -dataset POI/병원 -geom_col the_geom -srid EPSG:5186
##### 서울공공와이파이위치
hadoop fs -rm -r data/POI/서울공공와이파이위치
hadoop fs -copyFromLocal $MARMOT_DATA/기타/서울공공와이파이위치 data/POI
mc_bind_dataset -type csv data/POI/서울공공와이파이위치 -dataset POI/서울공공와이파이위치 -geom_col the_geom -srid EPSG:5186
##### 대규모점포
mc_import_shapefile %MARMOT_DATA%/사업단_자료/대규모점포 -dataset POI/대규모점포 -srid EPSG:5186 -charset euc-kr
##### 지진대피소
mc_import_shapefile %MARMOT_DATA%/사업단_자료/지진대피소 -dataset POI/지진대피소 -srid EPSG:5186 -charset utf-8
##### 지진발생 위치정보
mc_import_shapefile %MARMOT_DATA%/사업단_자료/지진발생_위치정보 -dataset POI/지진발생_위치정보 -srid EPSG:5186 -charset utf-8
##### 지구대_파출소
hadoop fs -rm -r data/POI/지구대_파출소
hadoop fs -copyFromLocal $MARMOT_DATA/공공데이터포털/지구대_파출소 data/POI
mc_bind_dataset data/POI/지구대_파출소 -dataset POI/지구대_파출소 -type csv -geom_col the_geom -srid EPSG:5186


############################################################################################
##### 소셜미디어 로그
############################################################################################
hadoop fs -mkdir data/로그/social
##### Twitter
hadoop fs -copyFromLocal $MARMOT_DATA/social/twitter_raw data/로그/social
mc_bind_dataset data/로그/social/twitter_raw -dataset 로그/social/twitter_raw -type custom -srid EPSG:4326 -geom_col the_geom
##### Instagram
hadoop fs -copyFromLocal $MARMOT_DATA/social/instagram_raw data/로그/social


############################################################################################
##### 데모용 샘플 데이터
############################################################################################
mc_import_shapefile %MARMOT_DATA%/사업단시연/서울_종합병원 -dataset 시연/hospitals -srid EPSG:5186 -charset utf-8
mc_import_shapefile %MARMOT_DATA%/사업단시연/화재사망자수_2015 -dataset 시연/fire_death -srid EPSG:5186 -charset utf-8
mc_import_shapefile %MARMOT_DATA%/사업단시연/시연용_서울특별시읍면동 -dataset 시연/demo_seoul -srid EPSG:5186 -charset utf-8
mc_import_shapefile %MARMOT_DATA%/행자부/법정구역_5179/시도/TL_SCCO_CTPRVN_11.shp -dataset 시연/서울특별시 -shp_srid EPSG:5179 -srid EPSG:5186 -charset euc-kr
mc_import_shapefile %MARMOT_DATA%/행자부/법정구역_5179/읍면동/TL_SCCO_EMD_11.shp -dataset 시연/서울_읍면동 -shp_srid EPSG:5179 -srid EPSG:5186 -charset euc-kr
mc_import_shapefile %MARMOT_DATA%/포스웨이브/대전공장_EPSG5174/tc7.shp -dataset 시연/대전공장 -charset euc-kr -shp_srid EPSG:5174 -srid EPSG:5186