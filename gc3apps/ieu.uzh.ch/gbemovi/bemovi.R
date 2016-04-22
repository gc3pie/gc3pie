args <- commandArgs(trailingOnly = TRUE)

library(bemovi)
to.data <- "data/"
to.particlelinker <- "/usr/local/ParticleLinker/"
IJ.path <- "/usr/local/ImageJ/ij.jar"
memory.alloc <- args[2]
fps <- args[3]
pixel_to_scale <- args[4]
difference.lag <- args[5]
thresholds = c(args[6],args[7])

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
locate_and_measure_particles(to.data, raw.video.folder, particle.data.folder, 
    difference.lag, thresholds, min_size = 5, max_size = 1000, IJ.path, memory.alloc)
}else{
link_particles(to.data, particle.data.folder, trajectory.data.folder, linkrange = 5, 
    disp = 20, start_vid = 1, memory = memory.alloc)
}