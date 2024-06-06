if [ $# -lt 1 ]; then
    echo 'give 1 argument'
else
    if [ $1 = 'list' ]; then
        curl-H "Content-type: application/json" -X POST -d '{"top_k":"10"}' http://10.255.81.132:10101/api-list >> ret
    fi
    
    if [ $1 = 'predict' ]; then
        curl-H "Content-type: application/json" -X POST -d '{"company_name":"삼성전자"}' http://10.255.81.132:10101/api-predict >> ret
    fi
fi
