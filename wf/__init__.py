#"""
#de novo transcriptome assembly
#"""

import subprocess
from pathlib import Path
import os

from latch import small_task, large_task, workflow
from latch.types import LatchFile, LatchDir


@small_task
def assemble_task(read1: LatchFile, read2: LatchFile, output_dir: LatchDir) -> (LatchFile, LatchFile):

	
	out_basename = str(output_dir.remote_path)
	transcripts = "/root/trinity_out_dir.Trinity.fasta"
	map = "/root/trinity_out_dir.Trinity.fasta.gene_trans_map"
	


	# command
	_trinity_cmd = [
        "Trinity",
        "--seqType",
        "fq",
        "--left",
        read1.local_path,
        "--right",
        read2.local_path,
        "--max_memory",
        "200G",
        "--full_cleanup"
        ]

	subprocess.run(_trinity_cmd)
	
	
	return (LatchFile(transcripts, 
	f"{out_basename}/trinity_out_dir.Trinity.fasta"),
	LatchFile(map, 
	f"{out_basename}/trinity_out_dir.Trinity.fasta.gene_trans_map"))

@workflow
def trinity(read1: LatchFile, read2: LatchFile, output_dir: LatchDir) -> (LatchFile, LatchFile):
    """

	# De novo Transcriptome Assembly
    
    This tool takes paired end transcriptome fastq files and outputs a fasta file with de novo assembled transcripts. You can learn more about Trinity on the wiki here - https://github.com/trinityrnaseq/trinityrnaseq/wiki#rna-seq-de-novo-assembly-using-trinity
	
    Trinity, developed at the Broad Institute and the Hebrew University of Jerusalem, 
	    represents a novel method for the efficient and robust de novo reconstruction of transcriptomes 
	    from RNA-seq data. Trinity combines three independent software modules: Inchworm, Chrysalis, and Butterfly, 
	    applied sequentially to process large volumes of RNA-seq reads. Trinity partitions the sequence data into 
	    many individual de Bruijn graphs, each representing the transcriptional complexity at a given gene or locus, 
	    and then processes each graph independently to extract full-length splicing isoforms and to tease apart transcripts 
	    derived from paralogous genes. Briefly, the process works like so:

    * __Inchworm__ assembles the RNA-seq data into the unique sequences of transcripts, often generating full-length 
	    transcripts for a dominant isoform, but then reports just the unique portions of alternatively spliced transcripts.

    * __Chrysalis__ clusters the Inchworm contigs into clusters and constructs complete de Bruijn graphs for each cluster. 
	    Each cluster represents the full transcriptonal complexity for a given gene (or sets of genes that share sequences in common). 
	    Chrysalis then partitions the full read set among these disjoint graphs.

    * __Butterfly__ then processes the individual graphs in parallel, tracing the paths that reads and pairs of reads take 
    	within the graph, ultimately reporting full-length transcripts for alternatively spliced isoforms, and teasing 
	    apart transcripts that corresponds to paralogous genes.



    __metadata__:
        display_name: De novo assembly of RNA seq data
        author:
            name: Corey Howe
            email: 	
            github: https://github.com/coreyhowe
        repository: https://github.com/coreyhowe/latch_trinity
        license:
            id: MIT

    Args:

        read1:
          Paired-end read 1 file to be assembled.

          __metadata__:
            display_name: Read1

        read2:
          Paired-end read 2 file to be assembled.

          __metadata__:
            display_name: Read2
            
        output_dir:
          The directory where results will go.
          
          __metadata__:
            display_name: Output Directory
    """
    
    return assemble_task(read1=read1, read2=read2, output_dir=output_dir)
    
    
#local iteration
#if __name__ == "__main__":
#    Trinity(read1=LatchFile("/Users/coreyhowe/Desktop/Corey/Science/Latch/old\ wf’s/rna_analysis/Data/A1_left.fq.gz"),
#    read2=LatchFile("/Users/coreyhowe/Desktop/Corey/Science/Latch/old\ wf’s/rna_analysis/Data/A1_right.fq.gz"),
#    output_dir=LatchDir("/Users/coreyhowe/Desktop/Corey/Science/Latch/Outputs/"))     
    