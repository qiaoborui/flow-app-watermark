from flask import Flask, request, jsonify
from lambda_function import lambda_handler
import json

app = Flask(__name__)

@app.route('/add-watermark', methods=['POST'])
def add_watermark():
    try:
        # 直接使用请求体
        event = {
            "body": request.get_data().decode('utf-8'),
            "isBase64Encoded": False
        }
        
        # 调用Lambda handler
        result = lambda_handler(event, None)
        
        # 返回处理结果
        return jsonify(json.loads(result['body'])), result['statusCode']
        
    except Exception as e:
        return jsonify({
            'message': 'Processing failed',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True,port=5001,host='0.0.0.0')
