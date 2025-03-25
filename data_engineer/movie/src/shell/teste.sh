pastas=("100" "200")

for str in "${pastas[@]}"; do
    if [[ "$str" == "100" ]]; then
        # echo "Erro: valor 100 encontrado"
        exit 1
    fi
done