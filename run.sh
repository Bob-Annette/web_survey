OPENAI_MODEL=Qwen3-8B
export OPENAI_API_KEY="empty"

python build_question_bank.py \
  --task value \
  --path-a /share/quant/wangjing/mysft/results/pred/value/Qwen3-8B/gatelora_4000/test_predictions.json \
  --path-b /share/quant/wangjing/mysft/results/pred/value/Qwen3-8B/moelora_4000/test_predictions.json \
  --num 10 \
  --out /share/quant/wangjing/mysft/streamlit/banks/value_bank.jsonl \
  --method-a-name A \
  --method-b-name B \
  --translate-zh \
  --openai-model ${OPENAI_MODEL} \
  --translate-cache cache/translate_cache.json

# python build_question_bank.py \
#   --task mic \
#   --path-a /share/quant/wangjing/mysft/results/pred/mic/Qwen3-8B/gatelora_4000/test_predictions.json \
#   --path-b /share/quant/wangjing/mysft/results/pred/mic/Qwen3-8B/moelora_4000/test_predictions.json \
#   --num 50 \
#   --out /share/quant/wangjing/mysft/streamlit/banks/mic_bank.jsonl \
#   --method-a-name A \
#   --method-b-name B \
#   --translate-zh \
#   --openai-model ${OPENAI_MODEL} \
#   --translate-cache cache/translate_cache.json


python split_question_bank.py \
  --bank /share/quant/wangjing/mysft/streamlit/banks/value_bank.jsonl \
  --m 5 \
  --n 2 \
  --seed 42


# python split_question_bank.py \
#   --bank /share/quant/wangjing/mysft/streamlit/banks/mic_bank.jsonl \
#   --m 20 \
#   --n 2 \
#   --seed 42

streamlit run app.py --server.address 0.0.0.0 --server.port 8501
