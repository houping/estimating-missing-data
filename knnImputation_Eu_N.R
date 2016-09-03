library(parallel)

# Initiate cluster
cl <- makeCluster(4)
uplimit <- 1
nc <-10
nm <-50 
x <- 1:nm
#data_normalized <- read.csv("C:/Users/pinghou/Downloads/RandomRemove/R/data_normalized.csv", header=FALSE)
data <- t(data_normalized[,1:nc])
m=ncol(data)
n=nrow(data)

## calculat error for each column
cal_error <- function(j,ind,distances_order,indexes) { 
  actualValues <- data[j,ind]
  errors <- vapply(2:n,function(k) { 
    weights <- distances_order[2:k,j]/sum(distances_order[2:k,j])
    y <- data[indexes[2:k,j],ind]
    if(length(weights)!=1) 
      y <- crossprod(data[indexes[2:k,j],ind], weights)
    error <- sqrt(mean((as.numeric(y) - actualValues)^2))
    return(error)},numeric(1))  
}

## estimate for randomly select number of x of missing data
estimate <- function(x,...) {
  ## randomly select missing data 
  ind <- sample(c(1:m),x)
  ## calculate distance between columns
  temp <- data[,-ind]
  distances <- as.matrix(dist(temp,upper = TRUE,diag = FALSE))
  distances_order <- vapply(1:n,function(j) distances[order(distances[,j]),j],numeric(n))
  indexes<- vapply(1:n,function(j) order(distances[,j]),numeric(n))
  ## find minmum error and according size of training data
  errors <- vapply(1:n,function(j) cal_error(j,ind,distances_order,indexes),numeric(n-1))
  
  minError <- vapply(1:n,function(j) min(errors[,j]),numeric(1))
  minErrorNumber <- vapply(1:n,function(j) min(which.min(errors[,j])),numeric(1))
  result=rbind(minError,minErrorNumber)
  return(result)}

clusterExport(cl,ls())
clusterEvalQ(cl,library(hydroGOF))


## estimate for different number of missing data
system.time(result_all <- parSapply(cl,rep(x,each=uplimit),function(xi) estimate(xi)))

stopCluster(cl)

minError <- result_all[seq(1, nrow(result_all), by = 2),]
minErrorNumber <- result_all[seq(2, nrow(result_all), by = 2),]
## reshape the matrix for each colunm represent x missing data
minError <-matrix(minError,n*uplimit,length(x),byrow=FALSE)
minErrorNumber <-matrix(minErrorNumber,n*uplimit,length(x),byrow=FALSE)

filename1 <- paste("minErrorN","_",nc,"_",nm,".csv",sep="")
filename2 <- paste("minErrorNumberN","_",nc,"_",nm,".csv",sep="")
write.csv(minError, file=filename1)
write.csv(minErrorNumber, file=filename2)
