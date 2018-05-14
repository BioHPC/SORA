#!/bin/bash

echo **Starting Time is `date`

# output directory
OUT_DIR=horseweed
OUT_FILE_PREFACE=Horseweed

# if input paired end reads are interleaved in same file
INPUT_INTERLEAVED_FILE= #/research/SparkMetagenomeAnalysis/assembler/datasets/CAMDA/TOK-001_interleaved.fasta.gz
# if input paired end reads are in different files
INPUT_READ1_FILE=/research/Yang/horseweed_project/data/Illumina_Pair_end/merged_1.fq.gz
INPUT_READ2_FILE=/research/Yang/horseweed_project/data/Illumina_Pair_end/merged_2.fq.gz
NODES=32

# Directory locations
BBmap=/opt/bbmap
SPARK=/research/software/spark/spark-2.1.0-bin-hadoop2.7/bin/

# Other files
ADAPTERS=${BBmap}/resources/adapters.fa
PHIX_ADAPTERS=${BBmap}/resources/phix174_ill.ref.fa.gz

# Start analysis
mkdir -p ${OUT_DIR}

# Trim using BBDuk from BBTools
TRIM_OUT=${OUT_DIR}/${OUT_FILE_PREFACE}_trim.fq.gz
TRIM_LOG=${OUT_DIR}/${OUT_FILE_PREFACE}_trim.log
if [ -f "${INPUT_INTERLEAVED_FILE}" ]
then
  echo "**Using a single interleaved file as input"
  ${BBmap}/bbduk.sh in=${INPUT_INTERLEAVED_FILE} out=${TRIM_OUT} int=t ftm=5 k=23 ktrim=r mink=11 hdist=1 tbo tpe qtrim=r trimq=10 minlen=70 ref=${ADAPTERS} 1>${TRIM_LOG} 2>&1 
elif [ -f "${INPUT_READ1_FILE}" ] && [ -f "${INPUT_READ2_FILE}" ]
then
  echo "**Using 2 paired end input files as input"
  ${BBmap}/bbduk.sh in1=${INPUT_READ1_FILE} in2=${INPUT_READ2_FILE} out=${TRIM_OUT} int=t ftm=5 k=23 ktrim=r mink=11 hdist=1 tbo tpe qtrim=r trimq=10 minlen=70 ref=${ADAPTERS} 1>${TRIM_LOG} 2>&1
else
  echo "**ERROR on the input files"
  exit 3
fi
echo **Finished Trimming `date`

# Filter
FILTER_OUT=${OUT_DIR}/${OUT_FILE_PREFACE}_filter.fq.gz
FILTER_LOG=${OUT_DIR}/${OUT_FILE_PREFACE}_filter.log
${BBmap}/bbduk.sh in=${TRIM_OUT} out=${FILTER_OUT} ref=${PHIX_ADAPTERS} hdist=1 k=31 1>${FILTER_LOG} 2>&1
echo **Finished Filtering `date`

# error correction and reformatting
EC_OUT=${OUT_DIR}/${OUT_FILE_PREFACE}_ec.fq.gz
EC_LOG=${OUT_DIR}/${OUT_FILE_PREFACE}_ec.log
RF_OUT=${OUT_DIR}/${OUT_FILE_PREFACE}_ec_rf.fq.gz
RF_LOG=${OUT_DIR}/${OUT_FILE_PREFACE}_ec_rf.log
${BBmap}/tadpole.sh in=${FILTER_OUT} out=${EC_OUT} mode=correct markbadbases=2 ecc=t shave rinse prealloc prefilter=2 minprob=0.8 1> ${EC_LOG} 2>&1
echo **Finished Error Correction `date`
${BBmap}/reformat.sh in=${EC_OUT} out=${RF_OUT} maxns=0 1> ${RF_LOG} 2>&1
echo **Finished Reformating `date`

# merge
MERGE_OUT=${OUT_DIR}/${OUT_FILE_PREFACE}_ec_rf_merged.fq.gz
UNMERGE_OUT=${OUT_DIR}/${OUT_FILE_PREFACE}_ec_rf_unmerged.fq.gz
MERGE_LOG=${OUT_DIR}/${OUT_FILE_PREFACE}_ec_rf_merge.log
${BBmap}/bbmerge.sh in=${RF_OUT} out=${MERGE_OUT} outu=${UNMERGE_OUT} 1> ${MERGE_LOG} 2>&1
echo **Finished merge `date`

DEDUPE_OUT=${OUT_DIR}/${OUT_FILE_PREFACE}_dedupe.fa
DEDUPE_LOG=${OUT_DIR}/${OUT_FILE_PREFACE}_dedupe.log
DEDUPE_GRAPH=${OUT_DIR}/${OUT_FILE_PREFACE}.graph
${BBmap}/dedupe.sh in=${MERGE_OUT} out=${DEDUPE_OUT} printlengthinedges=t fo=t pc=t fmj=f rc=f cc=f fcc=f foc=f mst=f dot=${DEDUPE_GRAPH} 1> ${DEDUPE_LOG} 2>&1
echo **Finished dedupe `date`

# generate dot file for SORA
DOT_LOG=${OUT_DIR}/${OUT_FILE_PREFACE}_dot_file.log
python parse_dot.py -i ${DEDUPE_GRAPH} 1> ${DOT_LOG} 2>&1
echo **Finished generating dot files `date`

# move them into output directory
mv ${OUT_FILE_PREFACE}_edge_list.txt ${OUT_DIR}/${OUT_FILE_PREFACE}_edge_list.txt
mv ${OUT_FILE_PREFACE}_vertex_list.txt ${OUT_DIR}/${OUT_FILE_PREFACE}_vertex_list.txt

# run ter
TER_OUT=${OUT_DIR}/${OUT_FILE_PREFACE}_TER_full.txt
TER_LOG=${OUT_DIR}/${OUT_FILE_PREFACE}_TER.log
${SPARK}/spark-submit --class TransitiveEdgeReduction --master local[${NODES}] transitive-edge-reduction-module_2.11-1.0.jar ${OUT_DIR}/${OUT_FILE_PREFACE}_edge_list.txt ${OUT_DIR}/TERResultParts/ 1> $TER_LOG 2>&1
# combine ter results
cat ${OUT_DIR}/TERResultParts/part-* > ${TER_OUT}
echo **Finished TER `date`

# run cec
CEC_OUT=${OUT_DIR}/${OUT_FILE_PREFACE}_CEC_full.txt
CEC_LOG=${OUT_DIR}/${OUT_FILE_PREFACE}_CEC.log
${SPARK}/spark-submit --class CompositeEdgeContraction --master local[${NODES}] Composite\ Edge\ Contraction\ Project-assembly-1.1.jar ${TER_OUT} ${OUT_DIR}/CECResultParts/ 1> ${CEC_LOG} 2>&1
cat ${OUT_DIR}/CECResultParts/part-* > ${CEC_OUT}
echo **Finished CEC `date`

echo **Ending Time is `date`


