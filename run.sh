OPENAI_MODEL=Qwen3-8B
export OPENAI_API_KEY="empty"
export TIDB_PASSWORD="Mthe4ORHpFICgs0Q"
export TIDB_CA="isrgrootx1.pem"

python build_question_bank.py \
  --task value \
  --path-a /share/quant/wangjing/mysft/results/pred/value/Qwen3-8B/gatelora_4000/test_predictions.json \
  --path-b /share/quant/wangjing/mysft/results/pred/value/Qwen3-8B/moelora_4000/test_predictions.json \
  --num 10 \
  --out /share/quant/wangjing/mysft/streamlit/data/banks/value_bank.jsonl \
  --method-a-name A \
  --method-b-name B \
  # --translate-zh \
  # --openai-model ${OPENAI_MODEL} \
  # --translate-cache data/cache/translate_cache.json

python build_question_bank.py \
  --task mic \
  --path-a /share/quant/wangjing/mysft/results/pred/mic/Qwen3-8B/gatelora_4000/test_predictions.json \
  --path-b /share/quant/wangjing/mysft/results/pred/mic/Qwen3-8B/moelora_4000/test_predictions.json \
  --num 10 \
  --out /share/quant/wangjing/mysft/streamlit/data/banks/mic_bank.jsonl \
  --method-a-name A \
  --method-b-name B \
  # --translate-zh \
  # --openai-model ${OPENAI_MODEL} \
  # --translate-cache data/cache/translate_cache.json


python split_question_bank.py \
  --bank /share/quant/wangjing/mysft/streamlit/data/banks/value_bank.jsonl \
  --m 5 \
  --n 3 \
  --seed 42


python split_question_bank.py \
  --bank /share/quant/wangjing/mysft/streamlit/data/banks/mic_bank.jsonl \
  --m 5 \
  --n 3 \
  --seed 42


python import_questionnaires_tidb.py \
  --banks_dir data/questions \
  --reset
  # --dry_run


# streamlit run app.py --server.address 0.0.0.0 --server.port 8501
streamlit run app_tidb.py --server.address 0.0.0.0 --server.port 8501

# python export_bank_style_from_tidb.py \
#   --output_dir export
