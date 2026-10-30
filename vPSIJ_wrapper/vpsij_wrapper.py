import cloudpickle
import pickle
import vPSIJ

from pathlib import Path
from datetime import timedelta
from typing import Optional, Union, Dict, List, Literal

class vPSIJ_wrapper:

  def __init__( self, sche, 
      work_directory: Optional[ Path ] = None,
      spec: Optional[ dict[ str, object ] ] = None
      #keep_files: Optional[ bool ] = False 
    ):
    print( sche )
    self.job = vPSIJ.Job( sche, work_directory = work_directory )
    self.executor = vPSIJ.Executor( self.job )
    if spec != None:
      self.job.spec = vPSIJ.job_spec( spec )

  def python_serialize_function_and_args( self, filename: Path, func_obj, args: list = [], kwargs: Dict = {} ) -> None:
    with open( filename, 'wb' ) as obj_file:
      cloudpickle.dump( ( func_obj, args, kwargs ), obj_file )

  def python_execute_script( self, func_obj_path: str, result_path: str ) -> str:
    return f"""
import sys

sys.dont_write_bytecode = True

import pickle
import sys

with open( '{ func_obj_path }', 'rb' ) as obj_file:
  func_obj, args, kwargs = pickle.load( obj_file )

results = None
exception = None

try:
  results = func_obj( *args, **kwargs )
except Exception as e:
  exception = e

with open( '{ result_path }', 'wb' ) as res_file:
  pickle.dump( ( results, exception ), res_File )
"""

  def config_spec(
    self,
    name: Optional[ str ] = None,
    directory: Optional[ Union[ str, Path ] ] = None,
    environments: Optional[ Dict[ str, object ] ] = None,
    executable: Optional[ str ] = None,
    arguments: Optional[ List[ str ] ] = None,
    pre_execution: Optional[ List[ str ] ] = None,
    post_execution: Optional[ List[ str ] ] = None,
    stdin_path: Optional[ Union[ str, Path ] ] = None,
    stdout_path: Optional[ Union[ str, Path ] ] = None,
    stderr_path: Optional[ Union[ str, Path ] ] = None,
    queue_name: Optional[ str ] = None,
    duration: Optional[ Union[ str, timedelta ] ] = None,
    node_count: Optional[ int ] = None,
    process_count: Optional[ int ] = None,
    process_per_node: Optional[ int ] = None,
    cpu_cores_per_process: Optional[ int ] = None,
    memory: Optional[ str ] = None,
    exclusive_node_use: bool = False,
    billing_account: Optional[ str ] = None,
    reservation_id: Optional[ str ] = None,
    custom_key_value: Optional[ Dict[ str, str ] ] = None,
    custom_value: Optional[ List[ str ] ] = None,
    cleanup_flag = None
    ):
    tmp = locals()
    del tmp[ 'self' ]
    self.job.spec = vPSIJ.job_spec( **tmp )

  # def submit( self ):
  #   self.executor.submit(
  #     secure_channel = False,
  #     callback_address: Literal[ 'hostname', 'ip', 'custom' ] = 'hostname',
  #     custom_callback_address = Z,
  #     custom_callback_port = ,
  #     updater_ssh_user_address = Y,
  #   )

  def submit_python( self,
    func_obj,
    args: list[ object ] = [],
    kwargs: Dict[ str, object ] = {},
    python_bin: Optional[ str ] = 'python',
    secure_channel = False,
    callback_address: Literal[ 'hostname', 'ip', 'custom' ] = 'hostname',
    custom_callback_address: Optional[ str ] = None,
    custom_callback_port: Optional[ str ] = None,
    updater_ssh_user_address: Optional[ str ] = None
    ):

    base_dir = self.job.work_directory
    job_id = self.job.id
    data_path = f'{ base_dir }/data/{ job_id }.pkl'
    pyscript_path = f'{ base_dir }/{ job_id }.py'
    func_obj_path = f'{ base_dir }/data/{ job_id }.pkl'
    result_path = f'{ base_dir }/result/{ job_id }_out.pkl'

    if Path( pyscript_path ).is_file() or Path( func_obj_path ).is_file():
      Path( pyscript_path ).unlink( missing_ok = True )
      Path( func_obj_path ).unlink( missing_ok = True )

    Path( base_dir ).mkdir( parents = True, exist_ok = True )
    Path( f'{ base_dir }/data' ).mkdir( parents = True, exist_ok = True )
    Path( f'{ base_dir }/result' ).mkdir( parents = True, exist_ok = True )

    self.job.spec.executable = f'{ python_bin } { pyscript_path }'

    self.python_serialize_function_and_args( data_path, func_obj, args = args, kwargs = kwargs )

    with open( pyscript_path, 'w' ) as f:
      f.write( self.python_execute_script( func_obj_path, result_path ) )

    self.executor.submit( 
      secure_channel = secure_channel,
      callback_address = callback_address,
      custom_callback_address = custom_callback_address,
      custom_callback_port = custom_callback_port,
      updater_ssh_user_address = updater_ssh_user_address
    )

    return { 'id': job_id, 'result_path': result_path }

  def remote_submit_python( self ):
    raise Exception( 'Under Development' )

  def wait_for_results( self, result_path ):
    status = self.executor.wait()
    return self.load_results( result_path )

  def load_results( self, result_path ):
    with open( result_path, 'rb' ) as f:
      output = pickle.load( f )
    return { 'results': output[0], 'error': output[1] }