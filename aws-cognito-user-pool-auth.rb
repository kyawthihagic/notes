class ApplicationController < ActionController::Base
  before_action :require_login
  attr_reader :current_user

  private

  def require_login
    pattern = /^Bearer /
    header = request.headers["Authorization"]
    if header && header.match(pattern)
      token = header.gsub(pattern, "")
      response = HTTParty.get("https://cognito-idp.us-east-1.amazonaws.com/us-east-1_GwOEQLaZP/.well-known/jwks.json")
      aws_idp = response.body
      jwt_config = JSON.parse(aws_idp, symbolize_names: true)
      begin
        @current_user = JWT.decode(token, nil, true, { jwks: jwt_config, algorithms: ["RS256"] })
      rescue => exception
        render json: { error: "Unauthorize" }, status: :unauthorized
      end
    else
      render json: { error: "Unauthorize" }, status: :unauthorized
    end
  end
end
