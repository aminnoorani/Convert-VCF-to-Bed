#!/bin/bash
#SBATCH --nodes=1
#SBATCH --time=1:00:00
#SBATCH --ntasks=40
#SBATCH --mail-type=FAIL

# Example job script to convert a vcf file to a bed file in parallel

# 1. Split up the file into fragments to be processed in parallel
#
#    Notes: - the l/240 syntax means 240 fragments without spliting lines
#           - more fragments, 240, than tasks, 40, will help load balancing
#           - $BB_JOB_DIR is a job-specific directory on the burst buffer file system
#             (see https://docs.scinet.utoronto.ca/index.php/Burst_Buffer)
#           - This produces fragments with prefix $BB_JOB_DIR/Fragment. followed by 
#             aaa, aab, etc, because of the --suffix-length=3

split --number l/240 --suffix-length=3 Altai.AllChrom.vcf $BB_JOB_DIR/Fragment.

# 2. Use GNU Parallel to process the parts in parallel.
#
#    Notes: - requires defining a function to operate on each chunck,
#             and which needs to be 'exported.
#           - See https://docs.scinet.utoronto.ca/index.php/Running_Serial_Jobs_on_Niagara
#

module load NiaEnv/2019b gnu-parallel

function process_fragment() 
{
    # This bash function converts a fragment of a vcf file fragment 
    # to bed format.  Takes one argument, the file to process, and
    # produces a file of the same name with an additional extension ".bed"
    grep -v "^#" $1 | awk '{printf "%s\t%d\t%d\t%s|%s|$s|%s|%s|%s|%s|%s|%s\n", $1,$2-1,$2,$3,$4,$5,$6,$7,$8,$9,$10}' > $1.bed
}

export -f process_fragment  # makes the function known to gnu parallel

parallel process_fragment ::: $BB_JOB_DIR/Fragment.???

# 3. Combine the processed parts 
#
#    Note: - "???" resolves alphabetically and therefore in the correct order.

cat $BB_JOB_DIR/Fragment.???.bed > Chr.bed

# Done
