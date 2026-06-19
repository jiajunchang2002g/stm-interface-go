for file in input/*.in; do
  base=$(basename "$file" .in)
  echo "Running test: $file"
  sleep 2
  ./grader-arm64 stminterface < "$file" > "output/${base}.out" 2>&1
  if [ $? -eq 0 ]; then
    echo "test passed" >> "output/${base}.out"
  else
    echo "test failed" >> "output/${base}.out"
  fi
done