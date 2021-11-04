##############################################################################################################
#                                                 zAsmt Data
#         Provides locational, jurisdictional, and descriptive information about the parcel/lot. 
#         It is possible for a given parcel to contain multiple properties. 
#         Join to Transaction tables using ImportParcelID. 
#         To Join to other Assessor tables, use ***RowID***.
##############################################################################################################
##############################################################################################################
#                                                 zTrans Data
#         Contains the primary record for various events 
#         Each transaction/event is uniquely identified by TransID.
#         Obtaining a clean set of arm's length consumer-to-consumer home sales will require extensive filtering on 
#         "DataClassStndCode, DocumentTypeStndCode, IntraFamilyTransferFlag, LoanTypeStndCode, 
#         and PropertyUseStndCode, potentially among others"
#         To match events to property attribute information in assessor tables, 
#         First join the Main table to PropertyInfo using ***TransID***. 
#         Use the ***ImportParcelID*** in the PropertyInfo table to link to the property in assessor tables. 
#         Use the ***ImportParcelID*** to match previous sales or other events on the same property. 
##############################################################################################################
##############################################################################################################
#                                                 ZTRAX Q&A
#         What variable best represents the true date of sale? (Recording Date vs Document Date vs something else)
#         ***Generally, we recommend using the DocumentDate. If missing, then Signature Date, if missing, then recording date. 
#         Are the ZAssessment tables generated from the ZTransaction tables? Why are sales prices provided in both databases?
#         ***When available, use the transaction data reported in the ZTransaction tables. 
##############################################################################################################

##### packages to load for reading data #####
library(readr)
library(dplyr)
library(readxl)
library(reshape)
library(data.table)
library(stringr)

#### packages to load for map plotting ####
library(sf)
library(raster)
library(rmapshaper)

#### packages for analysis and visulization ####
library(ggplot2)
library(zoo)

#################################################################################################
################################### IDENTIFY AREA OF INTEREST ###################################
#################################################################################################

setwd("/Users/cindy1992/Documents/LVO/ds543")
Aquifer = st_read("hp_bound2010.shp") 
#data source https://water.usgs.gov/GIS/metadata/usgswrd/XML/ds543.xml#stdorder
Aquifer1 <- ms_simplify(Aquifer, keep = 0.01, keep_shapes = T) # simplify the shp. boundary
setwd("/Users/cindy1992/Documents/LVO/cb_2018_us_county_20m")
County <- st_read("cb_2018_us_county_20m.shp") # read in county shp.
County <-st_transform(County, crs = projection(Aquifer1)) # uniform the crs
County$FIPS = paste0(County$STATEFP, County$COUNTYFP) # paste0: no space between chars.
county_OA = County[which(st_intersects(County, Aquifer1, sparse = FALSE)),] # 447 counties in the aquifer
OA_FIPS = as.numeric(county_OA$FIPS) # get all 447 county FIPS that overlie in the aquifer

# plot(st_geometry(Aquifer1))
# plot(st_geometry(county_OA), add = TRUE)



#################################################################################################
#################################### LOAD DATA FROM ZTRAX #######################################
#################################################################################################

#  Pull in layout information
dir <- "/Users/cindy1992/Documents/LVO/ZTRAX"
layoutZAsmt <- read_excel(file.path(dir, 'layout.xlsx'), sheet = 1)
layoutZTrans <- read_excel(file.path(dir, 'layout.xlsx'), sheet = 2)
####### get column names of ZAsmt and ZTrans tables (of interest)
col_ZAsmtMain <- layoutZAsmt[layoutZAsmt$TableName == 'utMain', 'FieldName']
col_ZAsmtBldg <- layoutZAsmt[layoutZAsmt$TableName == 'utBuilding', 'FieldName']
col_ZAsmtBldgA <- layoutZAsmt[layoutZAsmt$TableName == 'utBuildingAreas', 'FieldName']
col_ZTransMain <- layoutZTrans[layoutZTrans$TableName == 'utMain', 'FieldName']
col_ZTransPrIn <- layoutZTrans[layoutZTrans$TableName == 'utPropertyInfo', 'FieldName']


# my machine cannot read in files > 4GB,
# discussions also indicate that R does not provide reliable loading results when > 4GB
# unzip "48.zip" and then keep related files and zip into ZTrans.zip and ZAsmt.zip for TX

### 7 states (excluding TX)
folder = list.dirs(path = "/Volumes/DS2020/ZTRAX", full.names = TRUE, recursive = TRUE)
folder  # start from the second folder filepath

#### Define and read in only columns of interest ####
Mcols <- rep("NULL", 131) # columns of interest in Main 
# Mcols[c(1,2,3,4,5,7,17,18,19,25,26,27,28,29,30,31,33,34,63)] <- NA
# col_Main = col_ZTransMain[c(1,2,3,4,5,7,17,18,19,25,26,27,28,29,30,31,33,34,63),] # selected column names

Mcols[c(1,2,3,4,5,7,17,18,19,25,26,31,33,34)] <- NA
col_Main = col_ZTransMain[c(1,2,3,4,5,7,17,18,19,25,26,31,33,34),] # selected column names


Pcols <- rep("NULL", 68) # columns of interest in PropertyInfo
Pcols[c(1,16,17,18,19,20,21,23,46,47,53,54,63,65)] <- NA
col_PrIn = col_ZTransPrIn[c(1,16,17,18,19,20,21,23,46,47,53,54,63,65),] # selected column names

Names = c("08.zip","20.zip","31.zip","35.zip","40.zip",
          "46.zip","56.zip")
# State = c("Colorado","Kansas","Nebraska","New Mexico","Oklahoma",
#           "South Dakota","Wyoming")

# for (j in 1:length(Names)){ 
# j = 1 # CO
j = 3 # NE
# j = 5 # OK
  FileName = Names[j]; FileName
  #FileName = "ZTrans.zip" # TX
  MAIN = list()
  start_time = Sys.time()
  for (i in 2:length(folder)){ 
    setwd(folder[[i]]) #set working directory
    ####################collect variables from ZTrans\ZTrans.txt####################
    ## the filename only contains one backslash which is a defalt, 
    ## escape notation, need a second backslash to read the file
    ZTransMain <- read.csv(unz(FileName, "ZTrans\\Main.txt"), 
                           #ZTransMain <- read.csv(unz(FileName, filename="ZTrans/Main.txt"), # for TX
                           #nrows = rows2load, skip=rows2load*(round-1), # use if read in by chunk
                           colClasses=Mcols,
                           header=FALSE, 
                           sep = '|',
                           stringsAsFactors = FALSE,  
                           na.string = "NNN",  # SalesPriceAmountStndCode = 'NA' for non arm-length transaction, so need to use na.string to keep 'NA' string.
                           skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column 
                           comment.char="",                           # tells R not to read any symbol as a comment
                           quote = "")
    colnames(ZTransMain) <- col_Main[[1]] #11716836
    #ZTransMain <- as.data.table(ZTransMain) 
    ZTransMain <- ZTransMain[which(ZTransMain$FIPS %in% OA_FIPS),] # filtering for aquifer 
    ZTransMain$LandUse = substring(ZTransMain$AssessmentLandUseStndCode,1,2) # 1410176
    ZTransMain = ZTransMain[!(is.na(ZTransMain$SalesPriceAmount)),] # valid price input
    ZTransMain = ZTransMain[which(ZTransMain$SalesPriceAmount > 0),] # positive price
    ZTransMain[ZTransMain == ""] = NA ## replace balnk with NA
    ZTransMain[ZTransMain == " "] = NA  # 408269
    # remove observation with intrafamily trans flag
    ZTransMain = ZTransMain[is.na(ZTransMain$IntraFamilyTransferFlag),] # 404211
    # remove observation with unqualified sales price, keep ones with blank SalePriceCode
    NA1 = ZTransMain[is.na(ZTransMain$SalesPriceAmountStndCode),] # 58274
    NA0 = ZTransMain[!(is.na(ZTransMain$SalesPriceAmountStndCode)),] # 345937
    keep = NA0[!(NA0$SalesPriceAmountStndCode %in% c("NA","QU","ST","BL","RA")),] # 345750
    ZTransMain = rbind(NA1, keep) # 404024
    # remove observation with non arm-lenght deed
    dele1 = ZTransMain$DocumentTypeStndCode %in% c("AFDT","BFDE","CVDE","DEDB","GFDE","GRDE","INTR",
                                                      "JTDE","PRDE","PTDE","QCDE","SVDE","TFDD","TRFC",
                                                      "RCDE","BSDE","COCA","DELU","EXDE","AFDV","FCDE")
    ZTransMain = ZTransMain[!dele1,] # 387332
    ZTransMain = ZTransMain[!duplicated(ZTransMain),] # 404024
    MAIN[[i-1]] = ZTransMain
    rm(ZTransMain)
  }
    ### import PrIn data
    PRIN = list()
    for (i in 2:length(folder)){ 
      setwd(folder[[i]]) #set working directory
     ZTransPrIn <- read.csv(unz(FileName, "ZTrans\\PropertyInfo.txt"), 
                           #ZTransPrIn <- read.csv(unz(FileName, filename="ZTrans/PropertyInfo.txt"), # for TX
                           #nrows = rows2load, skip=rows2load*(round-1), # use if read in by chunk
                           colClasses=Pcols,
                           header=FALSE, 
                           sep = '|',
                           stringsAsFactors = FALSE,             
                           skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column 
                           comment.char="",                           # tells R not to read any symbol as a comment
                           quote = "")
    colnames(ZTransPrIn) <- col_PrIn[[1]] 
    ZTransPrIn =  ZTransPrIn[which(ZTransPrIn$FIPS %in% OA_FIPS),] # filtering for aquifer 
    ZTransPrIn = ZTransPrIn[!(is.na(ZTransPrIn$ImportParcelID)),] # need to have ParcelID
    ZTransPrIn = ZTransPrIn[!is.na(ZTransPrIn$LegalLotSize),] # 1364687
    PRIN[[i-1]] = ZTransPrIn
    rm(ZTransPrIn)
    }
  MAIN_TRANS = do.call(rbind, MAIN) # 2770630(CO); 2179502(NE)
  PRIN_TRANS = do.call(rbind, PRIN) # 11531559(CO); 7731117(NE)
  rm(MAIN)
  rm(PRIN)
  # CO takes about 2.23 hours
  MAIN_TRANS = MAIN_TRANS[which(MAIN_TRANS$SalesPriceAmount > 0),]  # (# 387332, CO);  2179502(NE)
  MAIN_TRANS$rec.year = as.numeric(substring(MAIN_TRANS$RecordingDate,1,4))
  MAIN_TRANS$doc.year = as.numeric(substring(MAIN_TRANS$DocumentDate,1,4))
  MAIN_TRANS$sig.year = as.numeric(substring(MAIN_TRANS$SignatureDate,1,4))
  MAIN_TRANS$year = ifelse(is.na(MAIN_TRANS$doc.year), MAIN_TRANS$sig.year, MAIN_TRANS$doc.year)
  MAIN_TRANS$year = ifelse(is.na(MAIN_TRANS$year), MAIN_TRANS$rec.year, MAIN_TRANS$year)
  MAIN_TRANS = MAIN_TRANS[!(is.na(MAIN_TRANS$year)),]  # 2770630  # (387332) 2179502(NE)
  MAIN_TRANS_dup = MAIN_TRANS[!duplicated(MAIN_TRANS),] # 426412 # (387332) 277582(NE)
  # MAIN_TRANS_dup$LandUse = ifelse(MAIN_TRANS_dup$PropertyUseStndCode == "AG", MAIN_TRANS_dup$PropertyUseStndCode, MAIN_TRANS_dup$land_code)
  # MAIN_TRANS_dup$LandUse = ifelse(is.na(MAIN_TRANS_dup$LandUse), MAIN_TRANS_dup$land_code, MAIN_TRANS_dup$LandUse)
  ##################################################################################
  end_time = Sys.time()
  end_time - start_time

  ##################################################################################
  ### use ASMT data for address 
  # location flag = 1, if full address or coordinates is available
  PRIN_TRANS[PRIN_TRANS == ""] = NA 
  PRIN_TRANS[PRIN_TRANS == " "] = NA 
  PRIN_TRANS$PropertyFullStreetAddress = ifelse(is.na(PRIN_TRANS$PropertyFullStreetAddress),
                                                    PRIN_TRANS$OriginalPropertyFullStreetAddress,
                                                    PRIN_TRANS$PropertyFullStreetAddress) # 11531559
  PRIN_TRANS$location = ifelse(is.na(PRIN_TRANS$PropertyFullStreetAddress) & is.na(PRIN_TRANS$PropertyAddressLatitude), 0, 1)
  
  # keep transaction whose location flag = 1 (with location info)
  PRIN_TRANS = PRIN_TRANS[which(PRIN_TRANS$location == 1),] # 11450481  (1364687 to 1359942) 7731117 to 7425121(NE)
  PRIN_TRANS = subset(PRIN_TRANS, select=-c(OriginalPropertyFullStreetAddress)) # for removing duplicated rows
  
  # transform the SF to AC for LegalLotSize
  out = strsplit(PRIN_TRANS$LegalLotSize, " ")
  PRIN_TRANS$size = as.numeric(sapply( out, "[", 1 )) # select the first element of strsplit
  PRIN_TRANS$unit = sapply( out, "[", 2 ) # select the second element of strsplit
  rm(out)
  # keep value if AC; SF usually is for residential to AC
  PRIN_TRANS = PRIN_TRANS[PRIN_TRANS$unit == "AC",] # 7647526; 5074856(NE)
  PRIN_TRANS$LotSizeAcres = PRIN_TRANS$size
  #PRIN_TRANS_dup$LotSizeAcres = ifelse(PRIN_TRANS_dup$unit == 'SF', PRIN_TRANS_dup$size/43560, PRIN_TRANS_dup$size) 
  # PRIN_TRANS$PropertyAddressLatitude = round(PRIN_TRANS$PropertyAddressLatitude,4)
  # PRIN_TRANS$PropertyAddressLongitude = round(PRIN_TRANS$PropertyAddressLongitude,4) # 3840446
  # id_prin_ag = unique(PRIN_TRANS_dup[which(PRIN_TRANS_dup$LotSizeAcres >= 30),"TransId"]) # keep transactions >= 30 acres
  # id_to_keep = union(id_main_ag, id_prin_ag) # get all the TransId that are possible AG land
  PRIN_TRANS_dup = PRIN_TRANS[!duplicated(PRIN_TRANS),] # 319975; 112899(NE)
  
  TRANS = merge(MAIN_TRANS_dup, PRIN_TRANS_dup, by = "TransId") # 172756; 74708(NE)
  rm(MAIN_TRANS_dup)
  rm(PRIN_TRANS_dup)
  TRANS_ag = TRANS[TRANS$LandUse == "AG",] # 18704; 7735(NE)
  TRANS_ag = TRANS_ag[complete.cases(TRANS_ag$SalesPriceAmount),] # 10850; 7735(NE)
  TRANS_30 = TRANS[is.na(TRANS$LandUse),] # 7854; 
  TRANS_30 = TRANS_30[TRANS_30$LotSizeAcres >= 30,] # 3251; 848(NE)
  TRANS_ST = rbind(TRANS_ag, TRANS_30) # 14101
  TRANS_ST = TRANS_ST[!duplicated(TRANS_ST),] # 14101; 8582(NE)
  

  TRANS_ST = subset(TRANS_ST, select=-c(FIPS.x, FIPS.y, LegalLotSize, RecordingDate, DocumentDate, SignatureDate, size, unit, TransId,
                                        PropertyAddressStndCode, DataClassStndCode, DocumentTypeStndCode, SalesPriceAmountStndCode, 
                                        PropertyUseStndCode,IntraFamilyTransferFlag, AssessmentLandUseStndCode, 
                                        rec.year, doc.year, sig.year, PropertySequenceNumber, PropertyZip4))
 TRANS_ST$PropertyFullStreetAddress = tolower( TRANS_ST$PropertyFullStreetAddress) # for removing duplicated rows
 TRANS_ST$PropertyCity = tolower( TRANS_ST$PropertyCity) # for removing duplicated rows
 TRANS_ST$PropertyState = tolower( TRANS_ST$PropertyState) # for removing duplicated rows
 TRANS_ST = TRANS_ST[!duplicated(TRANS_ST),] # 12204 # 7822(NE)
 TRANS_ST$PropertyFullStreetAddress = ifelse(TRANS_ST$PropertyFullStreetAddress == 'zzzz', NA, TRANS_ST$PropertyFullStreetAddress)
 TRANS_ST$PropertyFullStreetAddress = ifelse(TRANS_ST$PropertyFullStreetAddress == 'null', NA, TRANS_ST$PropertyFullStreetAddress)
 TRANS_ST = TRANS_ST[!(is.na(TRANS_ST$PropertyFullStreetAddress)),] # 8073; 4309(NE)
 
 
 TRANS_ST = TRANS_ST %>% 
   group_by(ImportParcelID, year) %>% # make sure each SequenceNumber of a given TransId has at most one input
   mutate(PropertyAddressLatitude = mean(PropertyAddressLatitude, na.rm=TRUE),
          PropertyAddressLongitude = mean(PropertyAddressLongitude, na.rm=TRUE),
          LotSizeAcres = mean(LotSizeAcres, na.rm=TRUE),
          SalesPriceAmount = sum(SalesPriceAmount, na.rm=TRUE),
          LotSizeAcres = sum(LotSizeAcres, na.rm=TRUE),
          PropertyFullStreetAddress =
            ifelse(is.na(max(nchar(PropertyFullStreetAddress))), 
                   PropertyFullStreetAddress[which.max(nchar(PropertyFullStreetAddress))], PropertyFullStreetAddress),
          LandUse = ifelse(is.na(max(nchar(LandUse))), LandUse[which.max(nchar(LandUse))], LandUse),
          PropertyCity = ifelse(is.na(max(nchar(PropertyCity))), PropertyCity[which.max(nchar(PropertyCity))], PropertyCity),
          PropertyZip = ifelse(length(unique(PropertyZip)) == 1, PropertyZip[which.max(nchar(PropertyZip))], NA),
          PropertyState = ifelse(is.na(max(nchar(PropertyState))), PropertyCity[which.max(nchar(PropertyState))], PropertyState)
   )
 
 TRANS_ST = TRANS_ST[!duplicated(TRANS_ST),] # 3233; 1573(NE)
  count =
    TRANS_ST %>%
    group_by(ImportParcelID, year) %>%
    tally() # n = 1
  summary(count)
  
  TRANS_ST = as.data.frame(TRANS_ST)
  
 # TRANS_CO = as.data.frame(TRANS_ST) # for Colorado
  TRANS_NE = as.data.frame(TRANS_ST) # for Nebraska
  setwd("/Users/cindy1992/Documents/LVO")
 # save(TRANS_CO, file = "TRANS_parcel_CO.Rda")
  save(TRANS_NE, file = "TRANS_parcel_NE.Rda")

  

  ##########################################################################################
  
  # Each transaction/event is uniquely identified by TransID. 
  # Each transaction may encompass multiple properties, 
  # each of which is assigned the same TransID. 
  # Properties that make up a transaction are sequenced by PropertySequenceNumber.
  # MAIN contains SaleAmount, so only keep PRIN with MAIN's TransId
  # PRIN_TRANS_ag = PRIN_TRANS_ag[!is.na(PRIN_TRANS_ag$LotSizeAcres),]
  # PRIN_TRANS_ag = subset(PRIN_TRANS_ag, select=-c(PropertyZip, PropertyZip4, LegalLotSize, size))
  # PRIN_TRANS_ag = PRIN_TRANS_ag[!duplicated(PRIN_TRANS_ag),]
  # PRIN_TRANS_ag = PRIN_TRANS_ag[PRIN_TRANS_ag$TransId %in% id,] # keep data with input in ZtransMain table

  data =  as.data.frame(TRANS_ST)
  id_add = unique(data[complete.cases(data$PropertyFullStreetAddress),"ImportParcelID"]) # 9779; 1464(NE)
  id_coor = unique(data[complete.cases(data$PropertyAddressLatitude),"ImportParcelID"]) # 12055; 1380(NE)
  id_google = unique(setdiff(id_add, id_coor)) # observations with address but not coordinates #84(NE)
  
  google = data[data$ImportParcelID %in% id_google,]
  
  #include city and zip whenever exist
  google$address = ifelse(is.na(google$PropertyCity),paste0(google$PropertyFullStreetAddress, ',', google$County,'County,', google$State)
                           ,paste0(google$PropertyFullStreetAddress, ',', google$PropertyCity,',', google$State))
  
  google$fulladdress = ifelse(is.na(google$PropertyZip),google$address
                          ,paste0(google$address, ',', google$PropertyZip))
  
  
  off_google = subset(data,!(ImportParcelID %in% id_google)) # 3067; 1488(NE)
  google = google[complete.cases(google$fulladdress),] # 166; 85(NE)
  
  rm(data)
  
  #register_google(key = "AIzaSyDIqO5HdLrBJuYRPkJ_n3O6kGqqyUPeUi8", write = T)
  library(ggmap)
  for(i in 1:nrow(google))
  {
    result <- geocode(google$address[i], output = "latlona", source = "google")
    google$PropertyAddressLongitude[i] <- as.numeric(result[1])
    google$PropertyAddressLatitude[i]  <- as.numeric(result[2])
    #address_dup$geoAddress[i] <- as.character(result[3])
  }
  google = google[complete.cases(google$PropertyAddressLatitude),] #
  
  
  count =
    google %>%
    group_by(PropertyAddressLatitude, PropertyAddressLongitude) %>%
    tally()

  ########## when the lat/lot repeat too many times, it's likely that the geocoding is not valid, remove those observations
  EX = count[count$n >=8,]
  check = google[!google$PropertyAddressLatitude == as.numeric(EX[1,1]),]
  check = check[!check$PropertyAddressLatitude == as.numeric(EX[2,1]),]
  
  google_valid = check
  google_valid = subset(google_valid, select = -c(address, fulladdress))  
  off_google$geocode = 0
  google_valid$geocode = 1
  TRANS_data = rbind(off_google, google_valid) # 3204
  TRANS_data = TRANS_data[!duplicated(TRANS_data),]
  count =
    TRANS_data %>%
    group_by(ImportParcelID, year) %>%
    tally() # n = 1
  summary(count)
  
  rm(off_google, google)
  #TRANS_geocode_CO = TRANS_data
  TRANS_geocode_NE = TRANS_data
  
  # setwd("/Users/cindy1992/Documents/LVO")
  # save(TRANS_geocode_CO, file = "TRANS_geocode_CO.Rda")
  setwd("/Users/cindy1992/Documents/LVO")
  save(TRANS_geocode_NE, file = "TRANS_geocode_NE.Rda")
  
  
  TRANS = TRANS_data[, c("ImportParcelID", "year", "PropertyAddressLatitude", "PropertyAddressLongitude", "LotSizeAcres", "SalesPriceAmount")]
  ### check ###
  count =
    TRANS %>%
    group_by(ImportParcelID, year) %>%
    tally() # n = 1
  summary(count)
 
  
  ############################################################################ 
  ###########################  read in ASMT data  ############################
  ############################################################################ 
  
  folder = list.dirs(path = "/Volumes/DS2020/ZTRAX", full.names = TRUE, recursive = TRUE)
  folder  # start from the second folder filepath
  
  
  dir <- "/Users/cindy1992/Documents/LVO/ZTRAX"
  layoutZAsmt <- read_excel(file.path(dir, 'layout.xlsx'), sheet = 1)
  col_ZAsmtMain <- layoutZAsmt[layoutZAsmt$TableName == 'utMain', 'FieldName']
  col_ZAsmtBldg <- layoutZAsmt[layoutZAsmt$TableName == 'utBuilding', 'FieldName']
  col_ZAsmtBldgA <- layoutZAsmt[layoutZAsmt$TableName == 'utBuildingAreas', 'FieldName']
  col_ZAsmtSale <- layoutZAsmt[layoutZAsmt$TableName == 'utSaleData', 'FieldName']
  #col_ZAsmtValue <- layoutZAsmt[layoutZAsmt$TableName == 'utValue', 'FieldName']
  
  AMcols <- rep("NULL", 95) # columns of interest in Main
  AMcols[c(1,2,3,4,5,13,27,28,30,70,82,83)] <- NA
  col_Main = col_ZAsmtMain[c(1,2,3,4,5,13,27,28,30,70,82,83),] # selected column names

  ABcols <- rep("NULL", 47) # columns of interest in PropertyInfo
  ABcols[c(1,6,19,20,46)] <- NA
  col_Bldg = col_ZAsmtBldg[c(1,6,19,20,46),] # selected column names
  
  ABAcols <- rep("NULL", 7) # columns of interest in PropertyInfo
  ABAcols[c(1,3,4,5,6)] <- NA
  col_BldgA = col_ZAsmtBldgA[c(1,3,4,5,6),] # selected column names
  
  AScols <- rep("NULL", 15) # columns of interest in PropertyInfo
  AScols[c(1,5,6,11,12,13)] <- NA
  col_Sale = col_ZAsmtSale[c(1,5,6,11,12,13),] # selected column names
  
  Names = c("08.zip","20.zip","31.zip","35.zip","40.zip",
            "46.zip","56.zip")
  
  # j = 1 # CO
  j = 3 # NE
  # j = 5 # OK
  
  FileName = Names[j]; FileName
  #FileName = "ZTrans.zip" # TX
  MAIN = list()
  BLDG = list()
  BLDA = list()
  SALE = list()
  start_time = Sys.time()
  for (i in 2:length(folder)){ 
    setwd(folder[[i]]) #set working directory
    ####################collect variables from ZTrans\ZTrans.txt####################
    ## the filename only contains one backslash which is a defalt, 
    ## escape notation, need a second backslash to read the file
    ZAsmtMain <- read.csv(unz(FileName, "ZAsmt\\Main.txt"), 
                          #ZAsmtMain <- read.csv(unz(FileName, filename="ZAsmt/Main.txt"), # for TX
                          #nrows = rows2load, skip=rows2load*(round-1), # use if read in by chunk
                          colClasses=AMcols,
                          header=FALSE, 
                          sep = '|',
                          stringsAsFactors = FALSE,             
                          skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column 
                          comment.char="",                           # tells R not to read any symbol as a comment
                          quote = "")
    colnames(ZAsmtMain) = col_Main[[1]] #2687364
    ZAsmtMain = ZAsmtMain[which(ZAsmtMain$FIPS %in% OA_FIPS),] # 426647
    ZAsmtMain = ZAsmtMain[which(ZAsmtMain$LotSizeAcres > 0),] # 377925
    ZAsmtMain$PropertyFullStreetAddress = str_squish(ZAsmtMain$PropertyFullStreetAddress) # uniform address format: reduce unexpected space
    ZAsmtMain$PropertyFullStreetAddress =  tolower(ZAsmtMain$PropertyFullStreetAddress) # uniform address format: lower case all letters
    ZAsmtMain$County =  tolower(ZAsmtMain$County) # uniform address format: lower case all letters
    ZAsmtMain$State =  tolower(ZAsmtMain$State) # uniform address format: lower case all letters
    ZAsmtMain = ZAsmtMain[!duplicated(ZAsmtMain),] # remove duplicated inputs # 377925
    MAIN[[i-1]] = ZAsmtMain
    rm(ZAsmtMain)
  }
  ASMT_main = do.call(rbind, MAIN) # 3502340
  ASMT_main[ASMT_main == ""] = NA 
  ASMT_main[ASMT_main == " "] = NA
  ASMT_main$PropertyFullStreetAddress = ifelse(ASMT_main$PropertyFullStreetAddress == 'zzzz', NA, 
                                               ASMT_main$PropertyFullStreetAddress)
  ASMT_main$PropertyFullStreetAddress = ifelse(ASMT_main$PropertyFullStreetAddress == 'null', NA, 
                                               ASMT_main$PropertyFullStreetAddress)
  ASMT_main$location = ifelse(is.na(ASMT_main$PropertyFullStreetAddress) & is.na(ASMT_main$PropertyAddressLatitude), 0, 1)
  
  # keep transaction whose location flag = 1 (with location info)
  ASMT_main = ASMT_main[which(ASMT_main$location == 1),]  # 3037054
  ASMT_main = ASMT_main[!duplicated(ASMT_main),] # 1035781
  setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
  # btitle = paste0("ASMT_MAIN_CO.Rda")
  btitle = paste0("ASMT_MAIN_NE.Rda")
  save(ASMT_main, file = btitle)
  end_time = Sys.time()
  end_time - start_time
  
  ASMT_main = ASMT_main %>% 
    group_by(RowID) %>% # make sure each SequenceNumber of a given TransId has at most one input
    mutate(PropertyAddressLatitude = mean(PropertyAddressLatitude, na.rm=TRUE),
           PropertyAddressLongitude = mean(PropertyAddressLongitude, na.rm=TRUE),
           PropertyFullStreetAddress =
             ifelse(is.na(max(nchar(PropertyFullStreetAddress))), PropertyFullStreetAddress, 
                    PropertyFullStreetAddress[which.max(nchar(PropertyFullStreetAddress))]), # the group use the longest address within the group
           PropertyCity =
             ifelse(is.na(max(nchar(PropertyCity))), PropertyCity, 
                    PropertyCity[which.max(nchar(PropertyCity))]), # the group use the longest city within the group
           PropertyZip = ifelse(length(unique(PropertyZip)) == 1, PropertyZip[which.max(nchar(PropertyZip))], NA) # keep zip if identical within the group
    )
  
  ASMT_main = ASMT_main[!duplicated(ASMT_main),] # 1020541
  n_occur <- data.frame(table(ASMT_main$RowID)) 
  ID = n_occur[n_occur$Freq > 1,1] # 1639
  rm(n_occur)
  
  ASMT_main = subset(ASMT_main,!(RowID %in% ID)) # 1017263
  
  
  count = 
    ASMT_main %>%
    group_by(RowID) %>% 
    tally() # n = 1
  summary(count)
  
  setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
  # btitle = paste0("ASMT_MAIN_1_CO.Rda")
  btitle = paste0("ASMT_MAIN_1_NE.Rda")
  save(ASMT_main, file = btitle)
  
  
  for (i in 2:length(folder)){ 
    setwd(folder[[i]]) #set working directory
    ZAsmtBldg <- read.csv(unz(FileName, "ZAsmt\\Building.txt"), 
                          #ZAsmtBldg <- read.csv(unz(FileName, filename="ZAsmt/Building.txt"), # for TX
                          #nrows = rows2load, skip=rows2load*(round-1), # use if read in by chunk
                          colClasses=ABcols,
                          header=FALSE, 
                          sep = '|',
                          stringsAsFactors = FALSE,             
                          skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column 
                          comment.char="",                           # tells R not to read any symbol as a comment
                          quote = "")
    colnames(ZAsmtBldg) = col_Bldg[[1]] # 1213140
    ZAsmtBldg = ZAsmtBldg[which(ZAsmtBldg$FIPS %in% OA_FIPS),] # 952895
    ZAsmtBldg$LandUse = substring(ZAsmtBldg$PropertyLandUseStndCode,1,2) 
    BLDG[[i-1]] = ZAsmtBldg
    rm(ZAsmtBldg)
  }
  ASMT_bldg = do.call(rbind, BLDG) # 3977632
  ASMT_bldg = ASMT_bldg[which(ASMT_bldg$LandUse %in% c("AG","")),] # 696450
  rm(BLDG)
  ASMT_bldg = subset(ASMT_bldg, select = -c(TotalRooms, TotalBedrooms, PropertyLandUseStndCode))
  ASMT_bldg = ASMT_bldg[!duplicated(ASMT_bldg),] # 213715
  
  count = 
    ASMT_bldg %>%
    group_by(RowID) %>% 
    tally() # n = 1
  summary(count)
  
  # Room = ASMT_bldg %>%
  #   group_by(RowID) %>% 
  #   mutate(# A RowID has multiple BuildingAreaSequenceNumber
  #     # For a BuildingAreaSequenceNumber, BAT = sum(all of other BuildingAreaStndCode's area)
  #     TotalRooms = mean(TotalRooms, na.rm = TRUE),
  #     TotalBedrooms = mean(TotalBedrooms, na.rm = TRUE))
  # 
  # Room = subset(Room, select = -c(PropertyLandUseStndCode))
  # ASMT_bldg = Room[!duplicated(Room),] 
  
  setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
  # btitle = paste0("ASMT_BLDG_CO.Rda")
  btitle = paste0("ASMT_BLDG_NE.Rda")
  save(ASMT_bldg, file = btitle)
  
  start_time = Sys.time()
  for (i in 2:length(folder)){ 
    setwd(folder[[i]]) #set working directory  
    ZAsmtSale <- read.csv(unz(FileName, "ZAsmt\\SaleData.txt"), 
                          #ZAsmtSale <- read.csv(unz(FileName, filename="ZAsmt/SaleData.txt"), # for TX
                          #nrows = rows2load, skip=rows2load*(round-1), # use if read in by chunk
                          colClasses=AScols,
                          header=FALSE, 
                          sep = '|',
                          na.string = "NNN",  # SalesPriceAmountStndCode = 'NA' for non arm-length transaction, so need to use na.string to keep 'NA' string.
                          stringsAsFactors = FALSE,             
                          skipNul = TRUE,                            # tells R to treat two ajacent delimiters as dividing a column 
                          comment.char="",                           # tells R not to read any symbol as a comment
                          quote = "")
    colnames(ZAsmtSale) = col_Sale[[1]] # 4307979
    ZAsmtSale = ZAsmtSale[which(ZAsmtSale$SalesPriceAmount > 100),] # 2621144
    ZAsmtSale$rec.year = as.numeric(substring(ZAsmtSale$RecordingDate,1,4))
    ZAsmtSale$doc.year = as.numeric(substring(ZAsmtSale$DocumentDate,1,4))
    ZAsmtSale$year = ifelse(is.na(ZAsmtSale$doc.year), ZAsmtSale$rec.year, ZAsmtSale$doc.year)
    ZAsmtSale = ZAsmtSale[complete.cases(ZAsmtSale$year),] # 2554576
    ZAsmtSale = ZAsmtSale[which(ZAsmtSale$year >= 1970),] # 2547261; use S.T. from 1970
    ### DocumentTypeStndCode / SalePriceAmountStndCode filter insert here ###
    ZAsmtSale = subset(ZAsmtSale, !(ZAsmtSale$DocumentTypeStndCode %in% c("AFDT","BFDE","CVDE","DEDB","GFDE","GRDE","INTR",
                                                                          "JTDE","PRDE","PTDE","QCDE","SVDE","TFDD","TRFC",
                                                                          "RCDE","BSDE","COCA","DELU","EXDE","AFDV","FCDE"))) # 2366682
    ZAsmtSale[ZAsmtSale == ""] = NA 
    ZAsmtSale[ZAsmtSale == " "] = NA
    NA1 = ZAsmtSale[is.na(ZAsmtSale$SalesPriceAmountStndCode),] # 1
    NA0 = ZAsmtSale[!(is.na(ZAsmtSale$SalesPriceAmountStndCode)),] # 2366681
    keep = NA0[!(NA0$SalesPriceAmountStndCode %in% c("NA","QU","ST","BL","RA")),] # 2366681
    ZAsmtSale = rbind(NA1, keep) # 2366682
    ZAsmtSale = subset(ZAsmtSale, select = -c(DocumentDate, RecordingDate,DocumentTypeStndCode, 
                                              rec.year, doc.year, SalesPriceAmountStndCode))
    ZAsmtSale = ZAsmtSale[!duplicated(ZAsmtSale),] # 2360266
    SALE[[i-1]] = ZAsmtSale
    rm(ZAsmtSale)
  }
  ASMT_sale = do.call(rbind, SALE) # 17965663
  rm(SALE)
  ASMT_sale = ASMT_sale[!duplicated(ASMT_sale),] # 5973896
  end_time = Sys.time()
  
  setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
  # btitle = paste0("ASMT_SALE_CO_raw.Rda")
  btitle = paste0("ASMT_SALE_NE_raw.Rda")
  save(ASMT_sale, file = btitle)
  
  
  
  n_occur <- data.frame(table(ASMT_sale$RowID)) 
  ID = n_occur[n_occur$Freq > 1,1] # 1783057
  rm(n_occur)
  
  one = subset(ASMT_sale,!(RowID %in% ID)) # TransId that only show up once   # 2258624
  two = subset(ASMT_sale, RowID %in% ID) # TransId that only show up more than once   # 3715272
  
  check = two %>%
      group_by(RowID, year) %>% # unique lotsize, coordinates and address for a given RowID
      mutate(frequency = n())
  check = as.data.frame(check)
  keep2 = check[check$frequency == 1,] # 3346177
  keep2 = subset(keep2, select = -c(frequency))
  keep2 = rbind(keep2, one) # 5604801
  
  rm(one)
  rm(two)
  rm(check)
  
  setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
  # btitle = paste0("ASMT_SALE_CO_1.Rda")
  btitle = paste0("ASMT_SALE_NE_1.Rda")
  save(keep2, file = btitle)
  # count = two %>%
  #   group_by(RowID, year) %>%
  #   tally()
  # 
  # id2 = unlist(unique(count[which(count$n == 1),"RowID"])) # 173565
  # check = two[two$RowID %in% id2,] # 358029
  # count1 = check %>%
  #   group_by(RowID, year) %>%
  #   tally()
  # 
  # check = rbind(check, one) # 601547
  # data.row = two %>%
  #   group_by(RowID, year) %>% # unique lotsize, coordinates and address for a given RowID
  #   mutate( # impute with mean coordinates and acreage
  #     SalesPriceAmount = sum(SalesPriceAmount, na.rm=TRUE))
  # data.row = as.data.frame(unique(data.row)) # 5542789
  # ASMT_sale = rbind(one, data.row) # 5786307
  # rm(one)
  # rm(two)
  # rm(data.row)
  
  # count = 
  #   ASMT_sale %>%
  #   group_by(RowID, year) %>% 
  #   tally() # n = 1
  # summary(count)
  
  
  setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
  # btitle = paste0("ASMT_SALE_CO.Rda")
  btitle = paste0("ASMT_SALE_NE.Rda")
  save(ASMT_sale, file = btitle)
  
  ASMT_data = merge(ASMT_main, ASMT_bldg, by = "RowID") # 141503
  ### keep AG land and land with empty landuse & lot size > 30
  keep_ag = ASMT_data[ASMT_data$LandUse == 'AG',] # 141170
  keep_30 = ASMT_data[(ASMT_data$LandUse == '' & ASMT_data$LotSizeAcres >= 30),] # 271
  ASMT_data = rbind(keep_ag, keep_30) 
  ASMT_data = ASMT_data[!duplicated(ASMT_data),] # 141441; 40574(NE)
  
  ASMT_data = ASMT_data %>%
    group_by(RowID) %>% # unique lotsize, coordinates and address for a given RowID
    mutate( LandUse = ifelse(is.na(max(nchar(LandUse))), '' , 'AG')) # make sure each RowID has one and only one land use, either "" or "AG"
  ASMT_data = ASMT_data[!duplicated(ASMT_data),] #141441; 40574(NE)
  
  count = 
    ASMT_data %>%
    group_by(RowID) %>% 
    tally() # n = 1
  summary(count)
  
  # ASMT_data contains RowID attribute (no year), keeps is the sale data (no attribute)
  
  ASMT_data = merge(ASMT_data, keep2, by = "RowID") # 42736; 12452(NE)
  
  count = 
    ASMT_data %>%
    group_by(RowID, year) %>% 
    tally() # n = 1
  summary(count) # 12452
  
  setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
  # title = paste0("ASMT_RowID_CO.Rda")
  title = paste0("ASMT_RowID_NE.Rda")
  save(ASMT_data, file = title)
  
 # load("~/Documents/LVO/New ZTRAX/ASMT_RowID_CO.Rda")
  load("~/Documents/LVO/New ZTRAX/ASMT_RowID_NE.Rda")
  
  ### summarize data to ImportParcelID by year level
  ASMT_data = ASMT_data %>%
    group_by(ImportParcelID, year) %>% # unique lotsize, coordinates and address for a given RowID
    mutate( # impute with mean coordinates and acreage
      SalesPriceAmount = sum(SalesPriceAmount, na.rm=TRUE),
      LotSizeAcres = sum(LotSizeAcres, na.rm=TRUE),
      PropertyAddressLatitude = mean( PropertyAddressLatitude, na.rm=TRUE),
      PropertyAddressLongitude = mean(PropertyAddressLongitude, na.rm=TRUE),
      PropertyCity = ifelse(length(unique(PropertyCity)) == 1, PropertyCity, NA), # if different city for the same RowID, then delete city
      PropertyZip = ifelse(length(unique(PropertyZip)) == 1, PropertyZip, NA), # if different zip for the same RowID, then delete zip
      PropertyFullStreetAddress =
        ifelse(is.na(max(nchar(PropertyFullStreetAddress))), 
               PropertyFullStreetAddress[which.max(nchar(PropertyFullStreetAddress))], PropertyFullStreetAddress))
  
  ASMT_data = subset(ASMT_data, select= -c(RowID, ParcelSequenceNumber))
  
  
  ASMT_data = ASMT_data[!duplicated(ASMT_data),] #25444; 10658(NE)
  
  count = 
    ASMT_data %>%
    group_by(ImportParcelID, year) %>% 
    tally() # n = 1
  summary(count)
  
  data =  as.data.frame(ASMT_data)
  data$PropertyFullStreetAddress = ifelse(data$PropertyFullStreetAddress == 'null', NA, 
                                               data$PropertyFullStreetAddress)
  data$location = ifelse(is.na(data$PropertyFullStreetAddress) & is.na(data$PropertyAddressLatitude), 0, 1)
  
  
  data = data[data$location == 1,] # 25283; 10658(NE)
  id_add = unique(data[complete.cases(data$PropertyFullStreetAddress),"ImportParcelID"]) # 9370; 6965(NE)
  id_coor = unique(data[complete.cases(data$PropertyAddressLatitude),"ImportParcelID"]) # 21207; 8925(NE)
  id_google = unique(setdiff(id_add, id_coor)) # observations with address but not coordinates # 959; 329(NE)
  
  google = data[data$ImportParcelID %in% id_google,] # 1073; 373(NE)
  
  
  
  # include city and zip whenever exist
  google$address = ifelse(is.na(google$PropertyCity),paste0(google$PropertyFullStreetAddress, ',', google$County,'County,', google$State)
                          ,paste0(google$PropertyFullStreetAddress, ',', google$PropertyCity,',', google$State))

  google$fulladdress = ifelse(is.na(google$PropertyZip),google$address
                              ,paste0(google$address, ',', google$PropertyZip))
  
  # # get full address format
  # google$fulladdress = paste0(google$PropertyFullStreetAddress, ',', google$County,' county,', google$State)
  #                         
  off_google = subset(data,!(ImportParcelID %in% id_google)) # 26888
  google = google[complete.cases(google$fulladdress),] # 1236

  rm(data)
  
  #register_google(key = "AIzaSyDIqO5HdLrBJuYRPkJ_n3O6kGqqyUPeUi8", write = T)
  library(ggmap)
  for(i in 1:nrow(google))
  {
    result <- geocode(google$fulladdress[i], output = "latlona", source = "google")
    google$PropertyAddressLongitude[i] <- as.numeric(result[1])
    google$PropertyAddressLatitude[i]  <- as.numeric(result[2])
    #address_dup$geoAddress[i] <- as.character(result[3])
  }
  google = google[complete.cases(google$PropertyAddressLatitude),] # 1071
  
  
  count =
    google %>%
    group_by(PropertyAddressLatitude, PropertyAddressLongitude) %>%
    tally()
  
  EX = as.data.frame(count[count$n >1,])
  check = google[!google$PropertyAddressLatitude %in% c(as.numeric(EX[,1])),] # 479
  count =
    check %>%
    group_by(PropertyAddressLatitude, PropertyAddressLongitude) %>%
    tally()
  summary(count)
  
  google_valid = check # 479; # 162
  google_valid = subset(google_valid, select = -c(fulladdress, address))  
  off_google$geocode = 0
  google_valid$geocode = 1
  ASMT_data = rbind(off_google, google_valid) # 24689; 10447(NE)
  ASMT_data = ASMT_data[!duplicated(ASMT_data),] # 24689; 10447(NE)
  count =
    ASMT_data %>%
    group_by(ImportParcelID, year) %>%
    tally() # n = 1
  summary(count)
  
  count =
    ASMT_data %>%
    group_by(ImportParcelID) %>%
    tally() # n >= 1
  summary(count)
  
  rm(off_google, google)
  # ASMT_geocode_CO = ASMT_data # 24689
  ASMT_geocode_NE = ASMT_data # 10447
 
  setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
  # save(ASMT_geocode_CO, file = "ASMT_geocode_CO.Rda")
  save(ASMT_geocode_NE, file = "ASMT_geocode_NE.Rda")
  
  
  # ASMT = ASMT_geocode_CO[, c("ImportParcelID", "year", "PropertyAddressLatitude", "PropertyAddressLongitude", "LotSizeAcres", "SalesPriceAmount")]
  # TRANS = TRANS_geocode_CO[, c("ImportParcelID", "year", "PropertyAddressLatitude", "PropertyAddressLongitude", "LotSizeAcres", "SalesPriceAmount")]
  ASMT = ASMT_geocode_NE[, c("ImportParcelID", "year", "PropertyAddressLatitude", "PropertyAddressLongitude", "LotSizeAcres", "SalesPriceAmount")]
  TRANS = TRANS_geocode_NE[, c("ImportParcelID", "year", "PropertyAddressLatitude", "PropertyAddressLongitude", "LotSizeAcres", "SalesPriceAmount")]
  

  id_TRANS = unique(TRANS$ImportParcelID) # 2704; 1447(NE)
  id_ASMT = unique(ASMT$ImportParcelID) # 21645; 9087(NE)
  id_keep = unique(setdiff(id_ASMT, id_TRANS)) # 20270; 8496(NE)
  
  # when TRANS data available, use TRANS. Otherwise, use ASMT
  
  ZTRAX = ASMT[ASMT$ImportParcelID %in% id_keep,] # 22918; 9702(NE) # keep ASMT observations that does not have record in TRANS
  ZTRAX = rbind(ZTRAX, TRANS) # 26122; 11258(NE) # add TRANS data
  ZTRAX = ZTRAX[complete.cases(ZTRAX$PropertyAddressLatitude),] # 26111; 11252(NE)
  
  count =
    ZTRAX %>%
    group_by(ImportParcelID, year) %>%
    tally() # n = 1
  summary(count)
  
  count =
    ZTRAX %>%
    group_by(ImportParcelID) %>%
    tally() # n >= 1
  summary(count) # sale frequency ranges from 1 to 4
  
  n_occur <- data.frame(table(ZTRAX$ImportParcelID)) 
  ID = n_occur[n_occur$Freq > 1,1] # 2999; 1286(NE) repetead parcels
  ID = n_occur[n_occur$Freq == 1,1] # 19975; 8657(NE) non-repeated parcels
  length(unique(ZTRAX$ImportParcelID)) # 29974; 9943(NE)
  rm(n_occur)
  
  # ZTRAX_CO = ZTRAX  # 26111
  ZTRAX_NE = ZTRAX  # 11252
  
  setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
  # save(ZTRAX_CO, file = "ZTRAX_CO.Rda")
  # save(ZTRAX_NE, file = "ZTRAX_NE.Rda")
  
  # setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
  # save(ZTRAX_CO_rep, file = "ZTRAX_CO_rep.Rda")
  # 
############################################################################ 

 ###### spatial attribute: ST and irrigation
 load("~/Documents/LVO/New ZTRAX/ZTRAX_CO.Rda")
 ZTRAX = ZTRAX_CO
 
 # load("~/Documents/LVO/New ZTRAX/ZTRAX_NE.Rda") 
 # ZTRAX = ZTRAX_NE
 # State = ZTRAX[,c("ImportParcelID","PropertyAddressLongitude", "PropertyAddressLatitude", "LotSizeAcres", "year")]
 State = ZTRAX[,c("ImportParcelID","PropertyAddressLongitude", "PropertyAddressLatitude", "LotSizeAcres")]
 
 State = State[!duplicated(State),] 
 State_sf = st_as_sf(State, coords = c("PropertyAddressLongitude", "PropertyAddressLatitude"), crs = 4269, dim = "XY")
 class(State_sf)
 State_sf = st_transform( State_sf, crs = st_crs(Aquifer1))
 ZTRAX_OA = State_sf[which(st_intersects(State_sf, Aquifer1, sparse = FALSE)),] # 5758
 ZTRAX_OA = ZTRAX_OA[!st_is_empty(ZTRAX_OA),drop=FALSE] # 5757 # rows with empty geometry can't operate extract
 ZTRAX_OA = st_transform(ZTRAX_OA, crs = 26913)
 
# ZTRAX_OA_rep =  ZTRAX_OA[ ZTRAX_OA$ImportParcelID %in% ID,]
 
 
 # setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
 # st_write(ZTRAX_OA, "CO_ZTRAX_AT.shp")
 # setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
 # st_write(ZTRAX_OA, "NE_ZTRAX_AT.shp")
 
 # setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
 # st_write(ZTRAX_OA_rep, "CO_ZTRAX_AT_rep.shp")
 
 ### read in ST raster
 setwd("/Volumes/DS2020/HPA_sat_thickness_inputs")
 L <- list.files(pattern = "\\.tif$") #collect all tif files in HPA folder to a list
 L
 #create raster stack of all .tif files
 rasHPA = stack(L)
 
ST2016 = raster(L[[50]])
df.2016 = as.data.frame(ST2016) 

 
 setwd("/Users/cindy1992/Documents/LVO/cb_2018_us_state_20m")
 State = st_read("cb_2018_us_state_20m.shp")
 OA = State[State$STUSPS %in% c("CO","NE","WY","TX","SD","KS","NM","OK"),]
 OA = st_transform(OA, crs = st_crs(rasHPA))
 setwd("/Users/cindy1992/Documents/LVO/ds543")
 Aquifer = st_read("hp_bound2010.shp") 
 #data source https://water.usgs.gov/GIS/metadata/usgswrd/XML/ds543.xml#stdorder
 Aquifer = st_transform(Aquifer, crs = st_crs(rasHPA))
 Aquifer1 = ms_simplify(Aquifer, keep = 0.01, keep_shapes = T) # simplify the shp. boundary
 
 ### get parcels within the OA
 State_HPA = st_transform(State_sf, crs = projection(rasHPA))
 ZTRAX_OA = State_HPA[which(st_intersects(State_HPA, Aquifer1, sparse = FALSE)),] # 5683 # 5763
 ZTRAX_OA = ZTRAX_OA[!st_is_empty(ZTRAX_OA),drop=FALSE] # rows with empty geometry can't operate extract # 5681 # 5761(CO)
 
 stor = st_read("/Volumes/DS2020/OFR98-414/stor_value.shp")
 # .e00 file downloaded from  https://water.usgs.gov/GIS/metadata/usgswrd/XML/ofr98-414.xml#stdorder
 # then read e00 file in QGIS, and export .shp 
 cond = st_read("/Volumes/DS2020/OFR98-548/cond_value.shp")
 # .e00 file downloaded from https://water.usgs.gov/GIS/metadata/usgswrd/XML/ofr98-548.xml#stdorder
 # then read e00 file in QGIS, and export .shp 
 stor<-st_transform(stor, crs = projection(rasHPA))
 cond<-st_transform(cond, crs = projection(rasHPA))
 
 ZTRAX_OA_c = st_join(ZTRAX_OA, cond[c("MAJOR1","MINOR1")])
 names(ZTRAX_OA_c) = c("ImportParcelID","LotSizeAcres","MinCond","MaxCond","geometry")
 
 ZTRAX_OA_cs = st_join(ZTRAX_OA_c, stor[c("MAJOR1","MINOR1")])
 names(ZTRAX_OA_cs) = c("ImportParcelID","LotSizeAcres","MinCond","MaxCond","MinStor", "MaxStor","geometry")
 
 OA_cs = st_drop_geometry(ZTRAX_OA_cs)
 OA_cs_mean = 
   OA_cs %>% 
   group_by(ImportParcelID) %>% 
   summarize(Cond = (mean(MaxCond) + mean(MinCond)/2),
             Stor = (mean(MaxStor) + mean(MinStor)/2))
 
 plot(st_geometry(OA))
 plot(st_geometry(ZTRAX_OA), add = TRUE)
 plot(st_geometry(Aquifer1), add = TRUE)
 
 # assign raster value to parcels within OA
 HPAValue = extract(rasHPA, ZTRAX_OA)  # cannot work when empty geometry exists
 HPA_OA = cbind(ZTRAX_OA, HPAValue) # add the RowID to the raster extract output
 ### use ST2016 to approximate ST 2017, 2018, 2019
 HPA_OA$SatThick_20171 = HPA_OA$SatThick_20161
 HPA_OA$SatThick_20181 = HPA_OA$SatThick_20161
 HPA_OA$SatThick_20191 = HPA_OA$SatThick_20161
 
 HPA_ZTRAX_OA = st_drop_geometry(HPA_OA)
 # The melt generic in data.table has been passed a data.frame, 
 # but data.table::melt currently only has a method for data.tables
 # Please confirm your input is a data.table
 HPA_ZTRAX_OA = as.data.table(HPA_ZTRAX_OA) # reshape the dataset
 #HPA_ZTRAX_OA = na.approx(HPA_ZTRAX_OA) # replace NA with neighbor values
 mdHPA = melt(HPA_ZTRAX_OA, id = c("ImportParcelID"))
 mdHPA$year = substring(mdHPA$variable, 10, 13)
 names(mdHPA) = c("ImportParcelID","Var1","ST","year") # mdPHA contains ST info for parcels that falls into OA
 mdHPA.s = 
   mdHPA %>% 
   group_by(ImportParcelID, year) %>% 
   summarize(STvalue = mean(ST))
 mdHPA.s$year = as.numeric(mdHPA.s$year)
 
 ZTRAX_ST = ZTRAX[which(ZTRAX$ImportParcelID %in% ZTRAX_OA$ImportParcelID) ,] # select parcels that falls into OA
 ZTRAX_ST = merge(ZTRAX_ST, mdHPA.s, by = c("ImportParcelID","year"), all.x = TRUE) # 6276; 8680(NE)
 # ZTRAX_ST = ZTRAX_ST[which(ZTRAX_ST$year < 2017),] # no ST info after 2016 # 5621
 
 final = merge(ZTRAX_ST, OA_cs_mean, by = "ImportParcelID", all.x = TRUE) # 6276(CO); 8680(NE)
 final = final[!duplicated(final),] # 6276(CO); 8680(NE)
 final = final[complete.cases(final$STvalue),] # 6276(CO); 8679(NE)
 
 final$price_ac = final$SalesPriceAmount/final$LotSizeAcres
 # final2 = final[which(final$price_ac >= 100), ] # 5437
 df = read.csv("/Users/cindy1992/Documents/LVO/deflator.csv")
 final = merge(final, df, by.x = "year", by.y = "TIME")
 final$df.price_ac = 100*final$price_ac / final$GDP.DEF
 final$ST2 = final$STvalue*final$STvalue
 final$IC = final$STvalue*final$Cond
 setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
 save(final, file = "final0_CO.Rda")
 
 setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
 save(final, file = "final0_NE.Rda")
 
 
 # final$IS = final$STvalue*final$Stor
 # final$METER = sqrt(final$LotSizeAcres*4046.86)
 # 
 # CDSS_ZTRAX = st_read("/Users/cindy1992/Documents/LVO/New ZTRAX/CO_AT_irri.shp")
 
 # CDSS_ZTRAX = st_read("/Users/cindy1992/Documents/LVO/New ZTRAX/CO_AT_GW_irri.shp")
 
 #### for Colorado ####
 CDSS_ZTRAX = st_read("/Users/cindy1992/Documents/LVO/New ZTRAX/CO_AT_irri_GW.shp") # intersect with CSDD, include DIV info
 CDSS_ZTRAX = st_drop_geometry(CDSS_ZTRAX)
 CDSS_ZTRAX = CDSS_ZTRAX[,c(1,5:9)] # for CO only
 
 CDSS_ZTRAX =  CDSS_ZTRAX %>%
   group_by(ImprPID) %>%
   mutate(
     Aqbase_mea = mean(Aqbase_mea, na.rm = TRUE),
     Aqbase_maj = mean(Aqbase_maj, na.rm = TRUE),
     Elev_mean = mean(Elev_mean, na.rm = TRUE),
     Elev_major = mean(Elev_major, na.rm = TRUE))
 CDSS_ZTRAX = CDSS_ZTRAX[!duplicated(CDSS_ZTRAX),] # 1999 to 1996
 
 
 id_irri = unique(CDSS_ZTRAX$ImprPID) # 1924
 final$irrigated = 0
 final[final$ImportParcelID %in% id_irri, "irrigated"] = 1
 summary(final$irrigated) # mean 34.7%
 
 div_1 = unique(CDSS_ZTRAX[CDSS_ZTRAX$DIV_majori == 1,"ImprPID"]) # 1838
 div_2 = unique(CDSS_ZTRAX[CDSS_ZTRAX$DIV_majori == 2,"ImprPID"]) # 86
 final$DIV = 0
 final[final$ImportParcelID %in% div_2, "DIV"] = 2
 final[final$ImportParcelID %in% div_1, "DIV"] = 1
 
 setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
 #save(final, file = "final_CO.Rda")
 save(final, file = "final_CO_GW.Rda")
 load("~/Documents/LVO/New ZTRAX/final_CO_GW.Rda")
############################################################################################################
 
 GW = final[final$irrigated ==1,] # 2178  # (1938)
 GW = merge(GW, CDSS_ZTRAX, by.x = "ImportParcelID", by.y = "ImprPID", all.x = TRUE) # 2178   # (1938)
 # calculate the depth to water table (dtwt)
 # GW$dtwt = GW$Elev_mean - GW$Aqbase_mea + GW$STvalue
 
 load("~/Documents/LVO/New ZTRAX/ASMT_MAIN_CO.Rda")
 ASMT_FIPS = ASMT_main[,c(2,3)]
 rm(ASMT_main)
 ASMT_FIPS = unique(ASMT_FIPS)
 GW = merge(GW, ASMT_FIPS, by = "ImportParcelID", all.x  = TRUE) # 2178  # (1938)

 count =
   GW %>%
   group_by(ImportParcelID, year) %>%
   tally() # n = 1
 summary(count$n)
 
 reg.data = GW
 reg.data$YEAR = as.factor(reg.data$year) 
 reg.data$ImportParcelID = as.factor(reg.data$ImportParcelID)
 reg.data$FIPS = as.factor(reg.data$FIPS)
 
 ############################################################################################################
 #### Nebraska ####
 CDSS_ZTRAX = st_read("/Users/cindy1992/Documents/LVO/New ZTRAX/NE_AT_irri_GW.shp") # intersect with pivot 2005
 # extract AqBase and Elev data to each parcel
 # setwd("//Volumes/DS2020/elevation_NED30M_ne_3913831_01")
 # AL <- list.files(pattern = "\\.tif$"); AL #collect all evelation tif files for NE to a list
 # LL = list()
 
 setwd("/Users/cindy1992/Desktop/NE_HPA_elev/GW")
 AL <- list.files(pattern = "\\.tif$"); AL #collect all evelation tif files for NE to a list
 LL2 = list()
 
 start_time = Sys.time()
 for (i in 1:length(AL)){
   elev = raster(AL[[i]])
   CDSS_ZTRAX = st_transform(CDSS_ZTRAX, crs = st_crs(elev))
   ex <- extract(elev, CDSS_ZTRAX, fun= mean, na.rm=TRUE)
   # LL[[i]] = ex
   LL2[[i]] = ex
 }
 end_time = Sys.time()
 end_time - start_time
 data = do.call(cbind, LL2)
 data = as.data.frame(data)
 NE_elev = rowSums (data, na.rm = TRUE)
 
 CDSS_ZTRAX = cbind(CDSS_ZTRAX, NE_elev) # 3804
 
 Aqbase = raster("/Volumes/DS2020/DS777_pts_Depth2Bedrock_shp/Kansas bedrock elevation raster/Erin/AqBase_TopCon1.tif")
 CDSS_ZTRAX = st_transform(CDSS_ZTRAX, crs = st_crs(Aqbase))
 AqBase <- extract(Aqbase, CDSS_ZTRAX, fun= mean, na.rm=TRUE)
 CDSS_ZTRAX = cbind(CDSS_ZTRAX, AqBase)
 CDSS_ZTRAX = st_drop_geometry((CDSS_ZTRAX)) # 3804(NE)
 CDSS_ZTRAX = CDSS_ZTRAX[!duplicated(CDSS_ZTRAX),] # 3803(NE)
 load("~/Documents/LVO/New ZTRAX/final0_NE.Rda")
 
 # merge ZTRAX with pivot
 GW = merge(final, CDSS_ZTRAX, by.x = c("ImportParcelID", "LotSizeAcres"), by.y = c("ImprPID","LtSzAcr")) # 4180
 GW = GW %>%
   group_by(ImportParcelID, year) %>%
   mutate(AqBase = mean(AqBase),
          NE_elev = mean(NE_elev))
 GW = unique(GW) # 4182
 
 count =
   GW %>%
   group_by(ImportParcelID, year) %>%
   tally() # n = 1
 summary(count$n)
 reg.data = GW
 setwd("/Users/cindy1992/Documents/LVO/New ZTRAX")
 save(GW, file = "GW_NE.Rda")
 
 
 ############################################################################################################
 USDA <- read.csv("/Users/cindy1992/Documents/LVO/USDA land value/USDA_land_value.csv")
 df = read.csv("/Users/cindy1992/Documents/LVO/deflator.csv")
 USDA = merge(USDA, df, by.x = "Year", by.y = "TIME")
 USDA$real.value = 100*USDA$Value / USDA$GDP.DEF
 
 USDA$min = USDA$Value/4
 USDA$max = USDA$Value*4
qmax = quantile(reg.data$df.price_ac , 3/4); qmax
qmin = quantile(reg.data$df.price_ac , 1/4); qmin
 
qmax = quantile(reg.data$df.price_ac , 7/8); qmax
qmin = quantile(reg.data$df.price_ac , 1/8); qmin

 # CO = USDA[USDA$State == 'COLORADO',]
 NE = USDA[USDA$State == 'NEBRASKA',]
 
 # reg.data = merge(reg.data, CO[,c("Year","min","max")], by.x = "year", by.y = "Year", all.x = TRUE) # 1938
 reg.data = merge(reg.data, NE[,c("Year","min","max")], by.x = "year", by.y = "Year", all.x = TRUE) # 8679
 
 base = HPA_ZTRAX_OA[,c("ImportParcelID","SatThick_19701")]
 base.s = 
   base %>% 
   group_by(ImportParcelID) %>% 
   summarize(ST1970 = mean(SatThick_19701))
 base.s = as.data.frame(base.s)
 base.s = unique(base.s)
 reg.data = merge(reg.data, base.s, by = "ImportParcelID", all.x = TRUE) 
 reg.data$Base = ifelse(reg.data$ST1970 < 30, 0, 1)
 reg.data$BST = reg.data$Base*reg.data$STvalue
 # reg.data$diff = reg.data$Elev_mean - reg.data$Aqbase_mea
 
 reg.data1 = reg.data[reg.data$price_ac < reg.data$max & reg.data$price_ac > reg.data$min, ] # 5996
 reg.data2 = reg.data[reg.data$df.price_ac <qmax & reg.data$df.price_ac > qmin, ] # 6509(1/8); 4399(1/4)
 
 reg.data0 = reg.data2
 
 reg.data_0 = reg.data0[reg.data0$STvalue > 0,] # 8361
# reg.data_0 = reg.data_0[complete.cases(reg.data_0$FIPS),] # 1513 # (1329)
 
 n_occur <- data.frame(table(reg.data_0$year)) # frequency of each RowID across years
 # get RowID with repeated trans over year
 ID = n_occur[n_occur$Freq > 1,1] # 46
 reg.data2 = reg.data_0[reg.data_0$year %in% ID,] # 6359
 
 n_occur <- data.frame(table(reg.data2$ImportParcelID)) # frequency of each RowID across years
 # get RowID with repeated trans over year
 ID = n_occur[n_occur$Freq > 1,1] # 576
 reg.data3 = reg.data2[reg.data2$ImportParcelID %in% ID,] # 1163
 
 n_occur <- data.frame(table(reg.data3$year)) # frequency of each RowID across years
 # get RowID with repeated trans over year
 ID = n_occur[n_occur$Freq > 1,1] # 30
 reg.data4 = reg.data3[reg.data3$year %in% ID,] # 1157
 
 n_occur <- data.frame(table(reg.data4$ImportParcelID)) # frequency of each RowID across years
 # get RowID with repeated trans over year
 ID = n_occur[n_occur$Freq > 1,1] # 570
 reg.data5 = reg.data4[reg.data4$ImportParcelID %in% ID,] # 1151
 
 
 count =
   reg.data5 %>%
   group_by(ImportParcelID) %>%
   tally() # n >= 2
 summary(count$n)
 count =
   reg.data5 %>%
   group_by(year) %>%
   tally() # n >= 2
 summary(count$n)
 
 library(stargazer)
 stargazer(subset(reg.data5, select = c("year","STvalue","price_ac","df.price_ac",
                                "Cond", "Stor")),
           title="Summary Statistics for Repeated Transactions: NE", digits=1, omit.summary.stat = c("p25", "p75"),
           covariate.labels=c("Year","Saturated Thickness (m)","Nominal Land Value ($/acre)","Real Land Value ($/acre)",
                              "Conductivity","Specific Yield"))
 stargazer(subset(reg.data2, select = c("year","STvalue","price_ac","df.price_ac",
                                        "Cond", "Stor" )),
           title="Summary Statistics for All Transactions: NE", digits=1, omit.summary.stat = c("p25", "p75"),
           covariate.labels=c("Year","Saturated Thickness","Nominal Land Value ($/acre)","Real Land Value ($/acre)",
                              "Conductivity","Specific Yield"))
 
 
 library(plm)
 library(multiwayvcov) 
 library(lmtest) 
 library(stargazer)
 library(sandwich)
 library(GGally)
 #####################################################################################################################################
 #####################################################################################################################################
 
 ggpairs(reg.data2[,c("df.price_ac","STvalue","ST2","Cond","Stor","diff","Base","BST","year")])
 
 #####################################################################################################################################
 
 ############################ The pooled model  with lm function #############################
 # fit_lm0 = lm(df.price_ac ~ STvalue + ST2 + Cond + Stor + Aqbase_mea + Elev_mean, data = reg.data2)
 
 #library(clubSandwich)
 # pool_plm = plm(df.price_ac ~ STvalue + ST2 + Cond + Stor + diff, 
 #                data = reg.data2, 
 #                model = "pooling",
 #                index = c("ImportParcelID", "YEAR"),
 #                effect = "time")
 # coeftest(pool_plm, vcov=vcovHC(pool_plm, method="arellano",
 #                                type="HC1"))
 # vcovCR(mypanelmodel, PanelData$Owner_ID,type="CR2")
 
 ## note: plm pooling does not add individual FE nor the time FE
 
 pool_lm = lm(df.price_ac ~ STvalue + ST2 + Cond + Stor + diff + YEAR, data = reg.data2)
 coeftest(pool_lm, vcov=vcovHC(pool_lm, method="arellano",
                                type="HC1"))[1:6,]
 
 
 
 fit_lm2 = lm(df.price_ac ~ STvalue + ST2 + ST1970:STvalue + Cond + Stor + diff + YEAR, data = reg.data2)
 fit_lm3 = lm(df.price_ac ~ STvalue + ST2 + ST1970:STvalue + year:STvalue  + Cond + Stor + diff + YEAR, data = reg.data2)

 vcov_county <- multiwayvcov::cluster.vcov(fit_lm, 
                                         reg.data2$FIPS,
                                         use_white = F, 
                                         df_correction = F)
 
 # vcov_div <- multiwayvcov::cluster.vcov(fit_lm, 
 #                                       reg.data2$DIV,
 #                                       use_white = F, 
 #                                       df_correction = F)
 # vcov_tw <- multiwayvcov::cluster.vcov(fit_lm, 
 #                                       cbind(reg.data2$ImportParcelID, reg.data2$YEAR),
 #                                       use_white = F, 
 #                                       df_correction = F)
 
 coeftest(fit_lm, vcov_div)[1:6,] 
 coeftest(fit_lm0, vcov_div0)[1:7,] 
 

 coeftest(fit_lm, vcov_tw)[1:7,] 
 coeftest(fit_lm, vcov_county)[1:7,] 
 coeftest(fit_lm, vcov_div)[1:7,] 

 pooled <- plm(df.price_ac ~ STvalue + ST2 + BST + Cond + Stor + diff + FIPS:YEAR, 
               data = reg.data2, 
               index = c("ImportParcelID", "YEAR"), 
               model = "pooling")
 
 # Note how this is functionally identical to the lm() way 
 coeftest(pooled, vcov = vcovHC, type = "HC1")[1:7,]
 
 
 stargazer(fit_lm, fit_lm, fit_lm, digits = 2, df = FALSE, omit = c("ImportParcelID","YEAR"), 
           se = list(sqrt(diag(vcov_tw)), sqrt(diag(vcov_county)), sqrt(diag(vcov_div))),
           covariate.labels = c("Saturated Thickness (ST)","ST$^2$", "Base:ST", 
                                "Conductivity", "Specific Yield", "Bedrock to Surface" ,"Constant")
 )
 
 #####################################################################################################################################
 ############################ The FE model with lm function #############################

 fx_lm = lm(df.price_ac ~ STvalue + ST2 + BST + ImportParcelID + FIPS:YEAR, data = reg.data5)
 vcov_tw <- multiwayvcov::cluster.vcov(fx_lm, 
                                       cbind(reg.data5$ImportParcelID, reg.data5$YEAR),
                                       use_white = F, 
                                       df_correction = F)
 vcov_county <- multiwayvcov::cluster.vcov(fx_lm, 
                                           reg.data5$FIPS,
                                           use_white = F, 
                                           df_correction = F)
 vcov_div <- multiwayvcov::cluster.vcov(fx_lm, 
                                        reg.data5$DIV,
                                        use_white = F, 
                                        df_correction = F)
 
 coeftest(fx_lm, vcov_tw)[1:3,] 
 coeftest(fx_lm, vcov_county)[1:3,] 
 coeftest(fx_lm, vcov_div)[1:3,] 
 
 stargazer(fx_lm, fx_lm, fx_lm, digits = 2, df = FALSE, omit = c("ImportParcelID","YEAR"), 
           se = list(sqrt(diag(vcov_tw)), sqrt(diag(vcov_county)), sqrt(diag(vcov_div))),
           covariate.labels = c("Saturated Thickness (ST)","ST$^2$", "Base:ST", "Constant")
 )

 #####################################################################################################################################
 ############################ The FE model with plm function #############################
 
 fx = plm(df.price_ac ~ STvalue + ST2, 
          data = reg.data5, 
          index = c("ImportParcelID", "YEAR"), 
          model = "within", 
          effect = "twoways")
 coeftest(fx, vcov=vcovHC(fx, method="arellano",
                          type="HC1", cluster="group"))
 fx.se = sqrt(diag(vcovHC(fx, type="HC3")))
 
 
 fx_lm = lm(df.price_ac ~ STvalue + ST2 + ImportParcelID + YEAR, data = reg.data5)
 coeftest(fx_lm, vcov=vcovHC(fx_lm, method="arellano",
                             type="HC1", cluster="group"))[1:3,]

 
 fx1 = plm(df.price_ac ~ STvalue + ST2 + STvalue:ST1970 , 
          data = reg.data5, 
          index = c("ImportParcelID", "YEAR"), 
          model = "within", 
          effect = "twoways")
 coeftest(fx1, vcov=vcovHC(fx1, type="HC3"))[1:3,]
 fx.se1 = sqrt(diag(vcovHC(fx1, type="HC3")))
 
 
 fx2 = plm(df.price_ac ~ STvalue + ST2 + STvalue:ST1970 + STvalue:YEAR, 
           data = reg.data5, 
           index = c("ImportParcelID", "YEAR"), 
           model = "within", 
           effect = "twoways")
 coeftest(fx2, vcov=vcovHC(fx2, type="HC3"))
 fx.se2 = sqrt(diag(vcovHC(fx2, type="HC3")))
 
 
 ifx = plm(df.price_ac ~ STvalue + ST2 + FIPS:YEAR, 
          data = reg.data5, 
          index = c("ImportParcelID", "YEAR"), 
          model = "within", 
          effect = "individual")
 coeftest(ifx, vcov=vcovHC(ifx, type="HC3"))[1:3,]
 ifx.se = sqrt(diag(vcovHC(ifx, type="HC3")))
 
 ifx1 = plm(df.price_ac ~ STvalue + ST2 +  STvalue:ST1970 + FIPS:YEAR, 
           data = reg.data5, 
           index = c("ImportParcelID", "YEAR"), 
           model = "within", 
           effect = "individual")
 coeftest(ifx1, vcov=vcovHC(ifx1, type="HC3"))[1:3,]
 ifx.se1 = sqrt(diag(vcovHC(ifx1, type="HC3")))
 
 
 ifx2 = plm(df.price_ac ~ STvalue + ST2 + STvalue:ST1970 + STvalue:YEAR + FIPS:YEAR, 
           data = reg.data5, 
           index = c("ImportParcelID", "YEAR"), 
           model = "within", 
           effect = "individual")
 coeftest(ifx2, vcov=vcovHC(ifx2, type="HC3"))
 ifx.se2 = sqrt(diag(vcovHC(ifx2, type="HC3")))
 
 stargazer(fx, fx1, fx2, ifx, ifx1, ifx2, digits = 2, df = FALSE, omit = c("ImportParcelID","YEAR"), 
           se = list(fx.se, fx.se1, fx.se2, ifx.se, ifx.se1, ifx.se2)
 )
 
 
 #####################################################################################################################################
 ############################ The pooling model with plm function #############################
 check = reg.data2 %>%
   mutate(# A RowID has multiple BuildingAreaSequenceNumber
     # For a BuildingAreaSequenceNumber, BAT = sum(all of other BuildingAreaStndCode's area)
     m.ST = mean(STvalue, na.rm = TRUE))
 
 check$d.ST = check$STvalue -check$m.ST
 check$d.ST2 = check$d.ST*check$d.ST
 
 cf0 = lm(df.price_ac ~ d.ST + d.ST2 + Cond + Stor + diff + YEAR, data = check)
 cf.se0 = sqrt(diag(vcovHC(cf0, type="HC1")))
 coeftest(cf0, vcov=vcovHC(cf0, type="HC1"))[1:6,]
 
 
 fx0 = lm(df.price_ac ~ STvalue + ST2 + Cond + Stor + diff + YEAR, data = reg.data2)
 fx.se0 = sqrt(diag(vcovHC(fx0, type="HC1")))
 coeftest(fx0, vcov=vcovHC(fx0, type="HC1"))[1:6,]
 
 
 
 
 fx = lm(df.price_ac ~ STvalue + ST2 +  Cond + Stor + diff + FIPS:YEAR, data = reg.data2)
          
 coeftest(fx, vcov=vcovHC(fx, type="HC1"))[1:6,]
 fx.se = sqrt(diag(vcovHC(fx, type="HC1")))
 
 fx1 = lm(df.price_ac ~ STvalue + ST2 + STvalue:ST1970 + Cond + Stor + diff + FIPS:YEAR, data = reg.data2)
 coeftest(fx1, vcov=vcovHC(fx1, type="HC1"))[1:7,]
 fx.se1 = sqrt(diag(vcovHC(fx1, type="HC1")))
 
 
 fx2 = lm(df.price_ac ~ STvalue + ST2 + STvalue:ST1970 + Cond + Stor + diff + STvalue:YEAR + FIPS:YEAR, data = reg.data2)
 coeftest(fx2, vcov=vcovHC(fx2, type="HC1"))[1:7,]
 fx.se2 = sqrt(diag(vcovHC(fx2, type="HC1")))
 
 stargazer(fx0, fx, fx1, fx2, digits = 2, df = FALSE, omit = c("YEAR"), 
           se = list(fx.se0, fx.se, fx.se1, fx.se2)
 )
 
#####################################################################################################################################
 ############################ Test for model specification ############################# 
  
 plmtest(df.price_ac ~ STvalue + ST2, data=reg.data5, effect="twoways", type="ghm")
 pwtest(df.price_ac ~ STvalue + ST2, data=reg.data5)
 pbgtest(fx_plm, order = 2)
 pwartest(df.price_ac ~ STvalue + ST2, data=reg.data5)
 pcdtest(df.price_ac ~ STvalue + ST2, data=reg.data5)
 
 
 fx_plm <- plm(df.price_ac ~ STvalue + ST2 + FIPS:YEAR, 
                data = reg.data5, 
               index = c("ImportParcelID", "YEAR"), 
               model = "within", 
               effect = "individual")
 
 # Note how this is functionally identical to the lm() way 
 coeftest(fx_plm, vcov = vcovHC, type = "HC1")[1:2,]
 
 #robust_se = sqrt(diag(vcovHAC(fit_lm)))
 coeftest(fit_lm, vcovHAC(fit_lm))[1:7,] 
 # robust_se = coeftest(fit_lm, vcov_tw)[1:7,] 
 
 
 fx = plm(df.price_ac ~ STvalue + ST2 , 
          data = reg.data5, 
          index = c("ImportParcelID", "YEAR"), 
          model = "within", 
          effect = "twoways",
          group = c("DIV"))
 coeftest(fx, vcov=vcovHC(fx, type="HC1",cluster="group"))
 
 fx2 = plm(df.price_ac ~ STvalue + ST2 + YEAR:FIPS, 
          data = reg.data5, 
          index = c("ImportParcelID", "YEAR"), 
          model = "within", 
          effect = "individual",
          group = c("DIV"))
 coeftest(fx2, vcov=vcovHC(fx2, type="HC1",cluster="group"))[1:2,]
 
 stargazer(pool, fx, fx2, digits = 2, df = FALSE, omit = c("ImportParcelID","YEAR"), se = list(robust_se, robust_se.p),
           covariate.labels = c("Saturated Thickness (ST)","ST$^2$", "Base:ST",
                                "Conductivity", "Specific Yield", "Bedrock to Surface" ,"Constant")
 )
 
 
 check5 = reg.data5[complete.cases(reg.data5$FIPS),]
 check2 = reg.data2[complete.cases(reg.data2$FIPS),]
 
 
 pool = lm(df.price_ac ~ STvalue + ST2  + Cond + Stor + Aqbase_mea + Elev_mean + YEAR:FIPS, data = check2)

 vcov_tw <- multiwayvcov::cluster.vcov(pool, 
                                       cbind(check2$FIPS, check2$YEAR),
                                       use_white = F, 
                                       df_correction = F)
 robust_se = sqrt(diag(vcovHAC(pool)))
 
 fx = plm(df.price_ac ~ STvalue + ST2  , 
          data = check5, 
          index = c("ImportParcelID", "YEAR"), 
          model = "within", 
          effect = "twoways",
          group = c("DIV"))
 robust_se.p =  sqrt(diag(vcovHC(fx, method = "arellano", cluster = "group")))
 
 fx2 = plm(df.price_ac ~ STvalue + ST2 + YEAR:FIPS, 
           data = check5, 
           index = c("ImportParcelID", "YEAR"), 
           model = "within", 
           effect = "individual",
           group = c("DIV"))

 robust_se.p2 =  sqrt(diag(vcovHC(fx2, method = "arellano", cluster = "group")))
 
 fx3  = plm(df.price_ac ~ STvalue + ST2, 
                 data = check5, 
                 index = c("ImportParcelID", "YEAR"), 
                 model = "within", 
                 effect = "twoways")
 robust_se.p3 =  sqrt(diag(vcovHC(fx3, type = "HC3")))
 coeftest(fx3, vcovHC(fx3, type = "HC3"))
 
 
 stargazer(fx3, digits = 2, df = FALSE, omit = c("ImportParcelID"), 
           se = robust_se.p3)
 
 
 
 stargazer(pool, fx, fx2, fx3, digits = 2, df = FALSE, omit = c("ImportParcelID","YEAR"), 
           se = list(robust_se, robust_se.p, robust_se.p2, robust_se.p3))
 
 
 ###########################################################################
 ###########################################################################
 
 
 fit_plm <- plm(df.price_ac ~ STvalue + ST2 , 
                data = reg.data5, 
                index = c("ImportParcelID", "YEAR"), 
                model = "within", 
                effect = "twoways",
                group = c("DIV"))
 # SE corrected for both heteroskedasticity and serial correlation, cluster at DIV level

 robust_se.p =  sqrt(diag(vcovHC(fit_plm, method = "arellano", cluster = "group")))
 coeftest(fit_plm, vcovHC(fit_plm, method = "arellano"), cluster = "group")
 
 fit_plm <- plm(df.price_ac ~ STvalue + ST2 + FIPS:YEAR, 
                data = reg.data5, 
                index = c("ImportParcelID", "YEAR"), 
                model = "within", 
                effect = "twoways")
 coeftest(fit_plm, vcov = vcovHC, type = "HC1")


 

 fit_lm = lm(df.price_ac ~ STvalue + ST2 + Cond + Stor + Aqbase_mea + Elev_mean + YEAR + FIPS:YEAR, data = reg.data2)
 vcov_tw <- multiwayvcov::cluster.vcov(fit_lm, 
                                       cbind(reg.data2$ImportParcelID, reg.data2$YEAR),
                                       use_white = F, 
                                       df_correction = F)
 robust_se = sqrt(diag(vcovHAC(fit_lm)))
 coeftest(fit_lm, vcovHAC(fit_lm))[1:7,] 
 # robust_se = coeftest(fit_lm, vcov_tw)[1:7,] 
 
 
 stargazer(fit_lm, fit_plm, digits = 2, df = FALSE, omit = c("ImportParcelID","YEAR"), se = list(robust_se, robust_se.p),
           covariate.labels = c("Saturated Thickness (ST)","ST$^2$", 
                                "Conductivity", "Specific Yield", "Bedrock Elev.","Surface Elev." ,"Constant")
)
 
 
 
cfd0 <- miceadds::lm.cluster( data=reg.data2, formula= df.price_ac ~ STvalue + ST2 + YEAR + Cond + Stor + Aqbase_mea + Elev_mean,
                              cluster=c(reg.data2$DIV))
cdf1 = miceadds::lm.cluster( data=reg.data5, formula= df.price_ac ~ STvalue + ST2 + YEAR + ImportParcelID,
                             cluster=c(reg.data5$DIV))

cfd <- miceadds::lm.cluster( data=reg.data2, formula= df.price_ac ~ YEAR + Cond + Stor +  dtwt,
                             cluster=c(reg.data2$DIV))

fit_lm = lm(df.price_ac ~ STvalue + ST2 + YEAR + ImportParcelID, data = reg.data5)

texreg(list(cfd1), digits = 3, 
       stars = c(0.001, 0.01, 0.05), 
       #custom.coef.names=c("Constant", "Saturated Thickness (ST)","ST$^2$", 
       #                    "Conductivity", "Specific Yield", "Nebraska", "Oklahoma" ),
       omit.coef = "(YEAR)|(ImportParcelID)"  # don't present estimates for regressors contain these text
) 

 texreg(list(cfd0, cfd1), digits = 3, 
        stars = c(0.001, 0.01, 0.05), 
        #custom.coef.names=c("Constant", "Saturated Thickness (ST)","ST$^2$", 
        #                    "Conductivity", "Specific Yield", "Nebraska", "Oklahoma" ),
        omit.coef = "(YEAR)|(ImportParcelID)"  # don't present estimates for regressors contain these text
 ) 
 
 cfd1 <- miceadds::lm.cluster( data=reg.data2, formula= df.price_ac ~ STvalue + ST2 + YEAR + ImportParcelID,
                              cluster=c(reg.data2$County))
 
 cfd_2 <- miceadds::lm.cluster( data=reg.pure, formula= df.price_ac ~ STvalue + ST2 + YEAR + Cond + Stor,
                               cluster=c(reg.pure$County))
 cfd2_2 <- miceadds::lm.cluster( data=reg.pure2, formula= df.price_ac ~ STvalue + ST2 + ImportParcelID + YEAR,
                                cluster=c(reg.pure2$County))
 
 cfd <- miceadds::lm.cluster( data=reg.pure, formula= df.price_ac ~ STvalue + ST2 + YEAR + Cond + Stor,
                               cluster=c(reg.pure$County))
 cfd2 <- miceadds::lm.cluster( data=reg.pure2, formula= df.price_ac ~ STvalue + ST2 + ImportParcelID + YEAR,
                                cluster=c(reg.pure2$County))
 library(texreg)
 texreg(list(cfd0, cfd1, cfd, cfd2), digits = 3, 
        stars = c(0.001, 0.01, 0.05), 
        #custom.coef.names=c("Constant", "Saturated Thickness (ST)","ST$^2$", 
        #                    "Conductivity", "Specific Yield", "Nebraska", "Oklahoma" ),
        omit.coef = "(YEAR)|(ImportParcelID)"  # don't present estimates for regressors contain these text
 ) 
 
 