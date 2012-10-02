require 'sinatra'
require 'redis'
require 'json'
require 'pry'


redis_config = {
  host: "carp.redistogo.com",
  user: 'stuart.lynn',
  password: '50055ad2a7cae11eab2bef03612d44e6',
  port: 9195,
  thread_safe: true 
}

Jobs                   = JSON.parse IO.read('jobs.json')
RedisConnection        = Redis.new redis_config


def delete_all
  RedisConnection.keys("pending_job_*").each {|key| RedisConnection.del key}
  RedisConnection.keys("active_job_*").each {|key| RedisConnection.del key}
end

def inital_setup
  delete_all
  Jobs.each do |job|
    RedisConnection.set "pending_job_#{job['id']}", job.to_json
  end
end

def pending_jobs
  RedisConnection.keys("pending_job_*").collect {|key| JSON.parse RedisConnection.get(key)}
end

def active_jobs
  RedisConnection.keys("active_job_*").collect {|key| JSON.parse RedisConnection.get(key)}
end

def assign_unassigned_job(ip)
  pending_job_key = RedisConnection.keys("pending_job_*").sample
  if pending_job_key
    job = JSON.parse RedisConnection.get(pending_job_key)
    RedisConnection.del(pending_job_key)
    job['ip'] = ip
    result = job.to_json
    RedisConnection.set pending_job_key.gsub('pending', "active"), result
  end
  result
end

get '/job_request' do
  result = assign_unassigned_job(request.ip) 
  result ||= [404,nil,'no jobs left']
  result

end

get '/' do
  {active_jobs: active_jobs, pending_jobs: pending_jobs}.to_json
end

get '/reset' do 
  inital_setup
  redirect to('/')
end

inital_setup