link_particles <- function(particle.data, jar.particlelinker, traj_out.dir, linkrange = 1, disp = 10, start_vid = 1, memory = 512) {

  ## particle.data: input trajectory data with patter ijout.txt
  ## jar.particlelinker: location of ParticleLinker.jar
  ## traj_out.dir: location of results
    
  to.data = "./tmp/"
  particle.data.name = basename(particle.data)

  time_to_load_particle <- system.time(PA_data <- read.table(particle.data, sep = "\t", header = T))
  cat("Time to load particle: [",time_to_load_particle,"]\n")

  ## Create result folder
  dir.create(traj_out.dir, showWarnings = F)
  
  ## only attempt particle linking if particles were detected in the video note: not sure what would happen if only one
  ## particle was found in one frame
  if (length(PA_data[, 1]) > 0) {

      # S3IT: why gsub over the .cxd pattern ?
      dir <- paste0(to.data, gsub(".cxd", "", sub(".ijout.txt", "", particle.data)))
      dir.create(dir,recursive=TRUE)
      
      for (i in 1:max(PA_data$Slice)) {
        frame <- subset(PA_data, Slice == i)[, c(6, 7)]
        frame$Z <- rep(0, length(frame[, 1]))
        sink(paste0(dir, "/frame_", sprintf("%04d", i - 1), ".txt"))
        cat(paste0("frame ", i - 1))
        cat("\n")
        sink()
        write.table(frame, file = paste0(dir, "/frame_", sprintf("%04d", i - 1), ".txt"), append = T, col.names = F, 
                    row.names = F)
      }
      
      ## run ParticleLinker
      if (.Platform$OS.type == "unix") {
        cmd <- paste0("java -Xmx", memory, "m -Dparticle.linkrange=", linkrange, " -Dparticle.displacement=", disp, 
                      " -jar ", " \"", jar.particlelinker,"\" ", "'", dir, "'", " \"", traj_out.dir,"/ParticleLinker_", 
                      particle.data.name,"\"")
        
        cat('Running: ', cmd,"\n")
        system(cmd)
      }
      
      if (.Platform$OS.type == "windows") {
        
        cmd <- paste0("C:/Progra~2/java/jre7/bin/javaw.exe -Xmx512m -Dparticle.linkrange=5 -Dparticle.displacement=20 -jar",
                      gsub("/","\\\\", paste0(" " ,jar.particlelinker)),
                      gsub("/","\\\\", paste0(" ","\"" ,dir,"\"")),
                      gsub("/","\\\\", paste0(" ","\"", traj_out.dir, "/ParticleLinker_", particle.data, "\"")))
        
        cat('Running: ', cmd,"\n")
        system(cmd)
      }
      

      ## delete working dir
      unlink(dir, recursive = TRUE)
      
  }
  
  if (length(PA_data[, 1]) == 0) {
      print(paste("***** No particles were detected in video", particle.data, " -- check the raw video and also threshold values"))
      
  }
}

args <- commandArgs(trailingOnly = TRUE)

if(length(args)!=4){
  print("Usage: Rscript --vanilla ParticleLinker.R <data_file> <particlelinker_jar_folder> <result_folder> <memory allocation>")
}

cat("Using arguments: data_file: ",args[1]," particleliker_jar_file: ",args[2], " result folder: ",args[3]," memory allocation: ",args[4]," \n")

link_particles(args[1], args[2], args[3], linkrange = 3, disp = 15, start_vid = 1, memory = args[4])
