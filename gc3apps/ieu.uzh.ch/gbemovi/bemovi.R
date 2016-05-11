args <- commandArgs(trailingOnly = TRUE)
# arg[1] = (locator|linker)
memory.alloc <- args[2]

if(args[1] == "locator" || args[1] == "linker"){
    fps <- as.numeric(args[3])
    pixel_to_scale <- eval(parse(text=args[4]))
    difference.lag <- as.numeric(args[5])
    thresholds = c(as.numeric(args[6]), as.numeric(args[7]))
}

library(bemovi)
to.data <- "data/"
to.particlelinker <- "/usr/local/ParticleLinker/"
IJ.path <- "/usr/local/ImageJ"

video.description.folder <- "1-raw/"
video.description.file <- "video.description.txt"
raw.video.folder <- "1-raw/"
particle.data.folder <- "2-particle/"
trajectory.data.folder <- "3-trajectory/"
temp.overlay.folder <- "4a-tmpoverlays/"
overlay.folder <- "4-overlays/"
merged.data.folder <- "5-merged/"
ijmacs.folder <- "ijmacs/"

if(args[1]=="locator"){
    locate_and_measure_particles(
        to.data,
        raw.video.folder,
        particle.data.folder, 
        difference.lag,
        thresholds,
        min_size = 5,
        max_size = 1000,
        IJ.path,
        memory.alloc)

}else if(args[1]=="linker"){
    link_particles(
        to.data,
        particle.data.folder,
        trajectory.data.folder,
        linkrange = 5, 
        disp = 20,
        start_vid = 1,
        memory = memory.alloc)

}else if(args[1]=="merger"){
    pfiles <- list.files(path=paste0(to.data, particle.data.folder), pattern='particle-.*\\.RData$')
    tfiles <- list.files(path=paste0(to.data, trajectory.data.folder), pattern='trajectory-.*\\.RData$')

    load(paste0(to.data, particle.data.folder, pfiles[1]))
    pdata <- morphology.data
    load(paste0(to.data, trajectory.data.folder, tfiles[1]))
    tdata <- trajectory.data

    for(index in seq(1, length(pfiles))){
        load(paste0(to.data, particle.data.folder, pfiles[index]))
        load(paste0(to.data, trajectory.data.folder, tfiles[index]))
        rbind(pdata, morphology.data)
        rbind(tdata, trajectory.data)
    }
    save(pdata, file=paste0(to.data, particle.data.folder, "particle.RData"))
    save(tdata, file=paste0(to.data, trajectory.data.folder, "trajectory.RData"))

    # We also need to merge all the RData files together
    merge_data(
        to.data,
        particle.data.folder,
        trajectory.data.folder,
        video.description.folder, 
        video.description.file,
        merged.data.folder)

}